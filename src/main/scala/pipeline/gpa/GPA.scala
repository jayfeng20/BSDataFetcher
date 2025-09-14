package pipeline.gpa

import brawlstars.model.BattleLogModel.{Brawler, Player}
import com.typesafe.scalalogging.Logger
import model.{GoodPlayerSummary, PlayerProfile}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import scala.collection.mutable

/** Good Players Augmenter (GPA)
  */
class GPA(config: conf.AppConfig) {
  private val logger        = Logger[this.type]
  private val LOGGER_PREFIX = "[GPA]"

  private val goodPlayersDir  = "data/good_players"
  private val goodPlayersFile = s"$goodPlayersDir/good_players_latest.json" // Top 1000 players
  private val allPlayersFile  = s"$goodPlayersDir/all_players_latest.json"  // All players

  private val maxGoodPlayers    = 1000
  private val inactivityDays    = 30      // Days after which a player is considered inactive
  private val maxPlayersToTrack = 1000000 // Maximum total players to track

  private val timestampFmt = DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH-mm-ss")
    .withZone(ZoneOffset.UTC)

  private val spark: SparkSession = SparkSession
    .builder()
    .appName("GoodPlayersAugmenter")
    .master("local[*]")
    .getOrCreate()

  sys.addShutdownHook {
    if (spark != null) {
      spark.stop()
      logger.info(s"$LOGGER_PREFIX Spark session stopped")
    }
  }

  private val glicko2System   = new Glicko2()
  private val trueSkillSystem = new TrueSkill()

  /** Archive old files with timestamp */
  private def archiveOldFiles(): Unit = {
    val ts         = java.time.LocalDate.now().toString
    val archiveDir = Paths.get(s"$goodPlayersDir/archived")

    if (!Files.exists(archiveDir)) Files.createDirectories(archiveDir)

    Seq(goodPlayersFile, allPlayersFile).foreach { file =>
      val oldFile = Paths.get(file)
      if (Files.exists(oldFile)) {
        val baseName = oldFile.getFileName.toString.replace("_latest.json", "")
        val archived = archiveDir.resolve(s"${baseName}_$ts.json")

        // If target exists
        if (Files.exists(archived)) {
          if (Files.isDirectory(archived)) {
            // Delete directory and everything inside
            import scala.jdk.CollectionConverters._
            Files
              .walk(archived)
              .iterator()
              .asScala
              .toSeq
              .reverse // delete children first
              .foreach(Files.delete)
          } else {
            Files.delete(archived)
          }
        }

        Files.move(oldFile, archived, StandardCopyOption.REPLACE_EXISTING)
        logger.info(s"Archived $file to $archived")
      }
    }
  }

  /** Load existing all players data or return empty map */
  private def loadAllPlayers(): Map[String, PlayerProfile] = {
    import spark.implicits._
    import scala3encoders.given

    try
      if (Files.exists(Paths.get(allPlayersFile))) {
        val profiles = spark.read
          .json(allPlayersFile)
          .as[PlayerProfile]
          .collect()
          .map(profile => profile.tag -> profile)
          .toMap

        logger.info(s"$LOGGER_PREFIX Loaded ${profiles.size} existing player profiles")
        profiles
      } else {
        logger.info(s"$LOGGER_PREFIX No existing player data found, starting fresh")
        Map.empty[String, PlayerProfile]
      }
    catch {
      case e: Exception =>
        logger.warn(s"$LOGGER_PREFIX Could not load existing players: ${e.getMessage}")
        Map.empty[String, PlayerProfile]
    }
  }

  /** Initialize a new player with default ratings */
  private def initializeNewPlayer(tag: String, firstSeen: java.sql.Timestamp): PlayerProfile =
    PlayerProfile(
      tag = tag,
      glicko2 = Glicko2Rating(),     // 1500 ±350, volatility 0.06
      trueSkill = TrueSkillRating(), // μ=25, σ=8.33
      battles = 0,
      wins = 0,
      losses = 0,
      draws = 0,
      lastSeen = firstSeen,
      firstSeen = firstSeen,
      combinedRating = 0.0, // Will be calculated after first battles
      isActive = true
    )

  /** Helper method to parse Brawler from Row or Map */
  private def parseBrawler(brawlerData: Any): Brawler =
    brawlerData match {
      case row: Row =>
        Brawler(
          id = row.getAs[Long]("id"),
          name = row.getAs[String]("name"),
          power = row.getAs[Int]("power"),
          trophies = row.getAs[Int]("trophies")
        )
      case map: Map[String, Any] @unchecked =>
        Brawler(
          id = map("id").asInstanceOf[Long],
          name = map("name").asInstanceOf[String],
          power = map("power").asInstanceOf[Int],
          trophies = map("trophies").asInstanceOf[Int]
        )
      case _ =>
        logger.warn(s"$LOGGER_PREFIX Unknown brawler data type: ${brawlerData.getClass}")
        Brawler(id = 0, name = "Unknown", power = 0, trophies = -1)
    }

  /** Extract team matchups and outcomes from battle data */
  private def extractMatchOutcomes(batchDF: DataFrame): Seq[(String, String, Double, java.sql.Timestamp)] = {
    import spark.implicits._
    import scala3encoders.given

    val matchData = batchDF.select($"perspectivePlayer", $"starPlayer", $"teams", $"result", $"battleTime").collect()

    matchData.flatMap { row =>
      try {
        val perspectivePlayer = row.getAs[String]("perspectivePlayer")

        val starPlayerRow = row.getAs[Row]("starPlayer")
        val starPlayer    = Player(
          tag = starPlayerRow.getAs[String]("tag"),
          name = starPlayerRow.getAs[String]("name"),
          // don't care about star player brawler for rating but parse anyway so future could use it
          brawler = parseBrawler(starPlayerRow.getAs[Row]("brawler"))
        )

        val teamsRaw = row.getAs[mutable.ArraySeq[mutable.ArraySeq[Row]]]("teams")
        val teams    = teamsRaw
          .map(_.toSeq)
          .toSeq
          .map(
            _.map(playerRow =>
              Player(
                tag = playerRow.getAs[String]("tag"),
                name = playerRow.getAs[String]("name"),
                null // don't care about brawler for rating
              )
            )
          )

        val result     = row.getAs[String]("result")
        val battleTime = row.getAs[java.sql.Timestamp]("battleTime")

        extractTeamOutcomes(perspectivePlayer, starPlayer, teams, result, battleTime)
      } catch {
        case e: Exception =>
          logger.warn(s"$LOGGER_PREFIX Failed to parse match data: ${e.getMessage}")
          Seq.empty
      }
    }.toSeq
  }

  /** Convert team battle results to individual player matchups */
  private def extractTeamOutcomes(
    perspectivePlayer: String,
    starPlayer: Player,
    teams: Seq[Seq[Player]],
    result: String,
    battleTime: java.sql.Timestamp
  ): Seq[(String, String, Double, java.sql.Timestamp)] = {
    if (teams.length < 2) return Seq.empty

    // Find which team the starPlayer belongs to
    val perspectivePlayerTeamIndex = teams.indexWhere(_.exists(_.tag == perspectivePlayer))
    if (perspectivePlayerTeamIndex == -1) {
      logger.warn(s"$LOGGER_PREFIX perspectivePlayer $perspectivePlayer not found in any team")
      println(perspectivePlayer)
      println(teams)
      teams.foreach(t => println(t.map(_.tag).mkString(", ")))
      return Seq.empty
    }

    val perspectivePlayerTeam = teams(perspectivePlayerTeamIndex)
    val opponentTeams         = teams.zipWithIndex.filter(_._2 != perspectivePlayerTeamIndex).map(_._1)

    // Convert result to numeric outcome from starPlayer's perspective
    val outcome = result.toLowerCase match {
      case "victory"      => 1.0
      case "defeat"       => 0.0
      case "draw" | "tie" => 0.5
      case _              =>
        logger.warn(s"$LOGGER_PREFIX Unknown result: $result, treating as draw")
        0.5
    }

    // Generate outcomes for all players on perspectivePlayer's team vs all opponents
    val allOutcomes = for {
      playerTeam   <- Seq(perspectivePlayerTeam) ++ opponentTeams.take(1) // Focus on main opponent team
      opponentTeam <- if (playerTeam == perspectivePlayerTeam) opponentTeams else Seq(perspectivePlayerTeam)
      player       <- playerTeam
      opponent     <- opponentTeam
      if player.tag != opponent.tag // Avoid self-matches
    } yield
      if (playerTeam == perspectivePlayerTeam) {
        // Player is on perspectivePlayer's team, use perspectivePlayer's outcome
        (player.tag, opponent.tag, outcome, battleTime)
      } else {
        // Player is on opponent team, reverse the outcome
        val reversedOutcome = outcome match {
          case 1.0 => 0.0     // perspectivePlayer won, so opponent lost
          case 0.0 => 1.0     // perspectivePlayer lost, so opponent won
          case 0.5 => 0.5     // draw remains draw
          case x   => 1.0 - x // general case
        }
        (player.tag, opponent.tag, reversedOutcome, battleTime)
      }

    allOutcomes
  }

  /** Update player ratings based on battle outcomes */
  private def updatePlayerRatings(
    allPlayers: Map[String, PlayerProfile],
    outcomes: Seq[(String, String, Double, java.sql.Timestamp)]
  ): Map[String, PlayerProfile] = {

    var updatedPlayers = allPlayers

    // Group outcomes by player
    val playerOutcomes = outcomes.groupBy(_._1)

    playerOutcomes.foreach { case (playerTag, playerMatches) =>
      val currentProfile = updatedPlayers.getOrElse(
        playerTag, {
          val firstBattleTime = playerMatches.map(_._4).min
          initializeNewPlayer(playerTag, firstBattleTime)
        }
      )

      // Count wins/losses/draws
      var newWins   = currentProfile.wins
      var newLosses = currentProfile.losses
      var newDraws  = currentProfile.draws

      // Prepare rating updates
      val opponentProfiles = playerMatches.map { case (_, opponentTag, outcome, battleTime) =>
        val opponentProfile = updatedPlayers.getOrElse(opponentTag, initializeNewPlayer(opponentTag, battleTime))

        // Count outcomes
        outcome match {
          case 1.0 => newWins += 1
          case 0.0 => newLosses += 1
          case 0.5 => newDraws += 1
          case _   => // Unknown outcome
        }

        (opponentProfile, outcome)
      }

      // Update Glicko-2 rating
      val opponents      = opponentProfiles.map(_._1.glicko2)
      val glickoOutcomes = opponentProfiles.map(_._2)
      val newGlicko2     = glicko2System.updateRating(currentProfile.glicko2, opponents, glickoOutcomes)

      // Update TrueSkill rating (simplified batch approach)
      val newTrueSkill = opponentProfiles.foldLeft(currentProfile.trueSkill) { case (ts, (opp, outcome)) =>
        outcome match {
          case x if x > 0.5 => trueSkillSystem.updateRating(ts, opp.trueSkill)._1
          case x if x < 0.5 => trueSkillSystem.updateRating(opp.trueSkill, ts)._2
          case _            => ts // No update for exact draws
        }
      }

      // Calculate combined rating (Glicko-2 weighted more heavily after establishment)
      val isEstablished   = newGlicko2.ratingDeviation < 100.0 && currentProfile.battles >= 20
      val glickoWeight    = if (isEstablished) 0.7 else 0.6
      val trueSkillWeight = 1.0 - glickoWeight

      val glickoNormalized    = (newGlicko2.rating - 1500.0) / 200.0 // Normalize around 0
      val trueSkillNormalized = (newTrueSkill.mu - 25.0) / 5.0       // Normalize around 0
      val combinedRating      = (glickoNormalized * glickoWeight) + (trueSkillNormalized * trueSkillWeight)

      val latestBattleTime = playerMatches.map(_._4).max

      val updatedProfile = currentProfile.copy(
        glicko2 = newGlicko2,
        trueSkill = newTrueSkill,
        battles = currentProfile.battles + playerMatches.length,
        wins = newWins,
        losses = newLosses,
        draws = newDraws,
        lastSeen = latestBattleTime,
        combinedRating = combinedRating,
        isActive = true
      )

      updatedPlayers = updatedPlayers.updated(playerTag, updatedProfile)
    }

    // Ensure all opponents are tracked (even if they didn't initiate matches)
    outcomes.foreach { case (_, opponentTag, _, battleTime) =>
      if (!updatedPlayers.contains(opponentTag)) {
        updatedPlayers = updatedPlayers.updated(opponentTag, initializeNewPlayer(opponentTag, battleTime))
      }
    }

    updatedPlayers
  }

  /** Mark inactive players and cleanup if needed */
  private def cleanupInactivePlayers(allPlayers: Map[String, PlayerProfile]): Map[String, PlayerProfile] = {
    val cutoffTime      = System.currentTimeMillis() - (inactivityDays * 24L * 60L * 60L * 1000L)
    val cutoffTimestamp = new java.sql.Timestamp(cutoffTime)

    val cleanedPlayers = if (allPlayers.size > maxPlayersToTrack) {
      logger.info(s"$LOGGER_PREFIX Player count (${allPlayers.size}) exceeds limit ($maxPlayersToTrack), cleaning up")

      // Mark inactive and remove lowest rated inactive players
      val (active, inactive) = allPlayers.partition { case (_, profile) =>
        profile.lastSeen.after(cutoffTimestamp)
      }

      val inactiveToKeep = inactive.toSeq
        .sortBy(_._2.combinedRating)(Ordering[Double].reverse)
        .take(maxPlayersToTrack - active.size)
        .map { case (tag, profile) => tag -> profile.copy(isActive = false) }
        .toMap

      active ++ inactiveToKeep
    } else {
      // Just mark inactive players
      allPlayers.map { case (tag, profile) =>
        val isActive = profile.lastSeen.after(cutoffTimestamp)
        tag -> profile.copy(isActive = isActive)
      }
    }

    logger.info(
      s"$LOGGER_PREFIX Player cleanup: ${cleanedPlayers.size} total, ${cleanedPlayers.count(_._2.isActive)} active"
    )
    cleanedPlayers
  }

  /** Generate top 1000 good players summary */
  private def generateGoodPlayersSummary(allPlayers: Map[String, PlayerProfile]): Seq[GoodPlayerSummary] = {
    val eligiblePlayers = allPlayers.values.filter { profile =>
      profile.isActive
    }

    val rankedPlayers = eligiblePlayers.toSeq
      .sortBy(_.combinedRating)(Ordering[Double].reverse)
      .take(maxGoodPlayers)
      .zipWithIndex
      .map { case (profile, index) =>
        val winRate    = if (profile.battles > 0) profile.wins.toDouble / profile.battles.toDouble else 0.0
        val daysActive = (profile.lastSeen.getTime - profile.firstSeen.getTime) / (1000L * 60L * 60L * 24L)

        GoodPlayerSummary(
          tag = profile.tag,
          rank = index + 1,
          glicko2Rating = profile.glicko2.rating,
          glicko2RD = profile.glicko2.ratingDeviation,
          trueSkillMu = profile.trueSkill.mu,
          trueSkillSigma = profile.trueSkill.sigma,
          combinedRating = profile.combinedRating,
          battles = profile.battles,
          winRate = winRate,
          lastSeen = profile.lastSeen,
          daysActive = daysActive
        )
      }

    logger.info(
      s"$LOGGER_PREFIX Generated top ${rankedPlayers.length} good players from ${eligiblePlayers.size} eligible players"
    )
    rankedPlayers
  }

  /** Save both all players and good players files */
  private def savePlayerData(allPlayers: Map[String, PlayerProfile], goodPlayers: Seq[GoodPlayerSummary]): Unit = {
    import spark.implicits._
    import scala3encoders.given

    try {
      // Save all players
      val allPlayersDF = spark.createDataset(allPlayers.values.toSeq).toDF()
      allPlayersDF
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .json(allPlayersFile)

      // Save good players
      val goodPlayersDF = spark.createDataset(goodPlayers).toDF()
      goodPlayersDF
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .json(goodPlayersFile)

      logger.info(s"$LOGGER_PREFIX Saved ${allPlayers.size} total players and ${goodPlayers.length} good players")
    } catch {
      case e: Exception =>
        logger.error(s"$LOGGER_PREFIX Failed to save player data", e)
        throw e
    }
  }

  /** Enhanced compute good players method */
  private def computeGoodPlayers(batchDF: DataFrame): Unit = {
    logger.info(s"$LOGGER_PREFIX Starting rating computation for batch")

    // Load existing player data
    val allPlayers = loadAllPlayers()

    // Extract match outcomes from batch
    val matchOutcomes = extractMatchOutcomes(batchDF)
    logger.info(s"$LOGGER_PREFIX Extracted ${matchOutcomes.length} match outcomes")

    if (matchOutcomes.nonEmpty) {
      // Update player ratings
      val updatedPlayers = updatePlayerRatings(allPlayers, matchOutcomes)

      // Clean up inactive players
      val cleanedPlayers = cleanupInactivePlayers(updatedPlayers)

      // Generate good players summary
      val goodPlayers = generateGoodPlayersSummary(cleanedPlayers)

      // Archive old files and save new data
      archiveOldFiles()
      savePlayerData(cleanedPlayers, goodPlayers)

      logger.info(s"$LOGGER_PREFIX Successfully updated player ratings and rankings")
    } else {
      logger.warn(s"$LOGGER_PREFIX No valid match outcomes found in batch")
    }
  }

  /** ======================================================================
    * Method currently not used - useful for when needing to
    * generate an initial good players file from a given set of good player tags,
    * visualize data,
    * recalculate good players from all players
    * ======================================================================
    */

  /** Generate good_players_init.json from good_players_seeds.json */
  def generateInitialGoodPlayersFromSeeds(): Unit = {
    logger.info(s"$LOGGER_PREFIX Generating initial good players from seed file")

    val seedsFile = s"$goodPlayersDir/good_players_seeds.json"
    val initFile  = s"$goodPlayersDir/good_players_init.json"

    try {
      import spark.implicits._
      import scala3encoders.given

      // Check if seeds file exists
      if (!Files.exists(Paths.get(seedsFile))) {
        logger.error(s"$LOGGER_PREFIX Seeds file not found: $seedsFile")
        throw new RuntimeException(s"Seeds file not found: $seedsFile")
      }

      // Read player tags from seeds file
      val seedsDF    = spark.read.json(seedsFile)
      val playerTags = seedsDF.select("tag").as[String].collect().toSeq

      if (playerTags.isEmpty) {
        logger.warn(s"$LOGGER_PREFIX No player tags found in seeds file")
        return
      }

      logger.info(s"$LOGGER_PREFIX Found ${playerTags.length} player tags in seeds file")

      val currentTime = new java.sql.Timestamp(System.currentTimeMillis())

      // Generate initial good player profiles with default ratings
      val initialGoodPlayers = playerTags.zipWithIndex.map { case (tag, index) =>
        // All players start with same base ratings (new player defaults)
        // Slight variation to establish initial ordering
        val ratingVariation = index * 0.1 // Very small variation
        val baseGlicko2     = 1500.0 + ratingVariation
        val baseTrueSkill   = 25.0 + (ratingVariation / 100.0)

        // Combined rating starts at 0 (neutral) with tiny variations
        val combinedRating = ((baseGlicko2 - 1500.0) / 200.0 * 0.6) + ((baseTrueSkill - 25.0) / 5.0 * 0.4)

        GoodPlayerSummary(
          tag = tag,
          rank = index + 1,
          glicko2Rating = baseGlicko2,
          glicko2RD = 350.0, // High uncertainty for new players
          trueSkillMu = baseTrueSkill,
          trueSkillSigma = 25.0 / 3.0, // High uncertainty for new players (~8.33)
          combinedRating = combinedRating,
          battles = 0,   // No battles yet
          winRate = 0.0, // No battles = no wins
          lastSeen = currentTime,
          daysActive = 0 // Just created
        )
      }

      // Save initial good players
      val goodPlayersDF = spark.createDataset(initialGoodPlayers).toDF()
      goodPlayersDF
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .json(initFile)

      logger.info(s"$LOGGER_PREFIX Successfully created $initFile with ${initialGoodPlayers.length} players")

      // Log the first few players for verification
      initialGoodPlayers.take(5).foreach { player =>
        logger.info(s"$LOGGER_PREFIX ${player.tag} - Rank: ${player.rank}, Rating: ${player.combinedRating}")
      }

    } catch {
      case e: Exception =>
        logger.error(s"$LOGGER_PREFIX Failed to generate initial good players from seeds", e)
        throw e
    }
  }

  /** Calculate and update top 1000 good players from existing all_players data */
  def recalculateGoodPlayers(): Unit = {
    logger.info(s"$LOGGER_PREFIX Starting recalculation of good players from existing data")

    try {
      // Load existing player data
      val allPlayers = loadAllPlayers()

      if (allPlayers.isEmpty) {
        logger.warn(s"$LOGGER_PREFIX No existing player data found, cannot recalculate good players")
        return
      }

      logger.info(s"$LOGGER_PREFIX Loaded ${allPlayers.size} players for recalculation")

      // Clean up inactive players (optional, can be skipped if you want to preserve all data)
      val cleanedPlayers = cleanupInactivePlayers(allPlayers)

      // Generate good players summary from current data
      val goodPlayers = generateGoodPlayersSummary(cleanedPlayers)

      if (goodPlayers.nonEmpty) {
        // Archive old files and save new data
        archiveOldFiles()
        savePlayerData(cleanedPlayers, goodPlayers)

        logger.info(s"$LOGGER_PREFIX Successfully recalculated and saved ${goodPlayers.length} good players")

        // Log some statistics
        val totalActive   = cleanedPlayers.count(_._2.isActive)
        val totalEligible = cleanedPlayers.count { case (_, profile) =>
          profile.isActive
        }
        val topPlayer = goodPlayers.headOption

        logger.info(s"$LOGGER_PREFIX Statistics:")
        logger.info(s"$LOGGER_PREFIX - Total players: ${cleanedPlayers.size}")
        logger.info(s"$LOGGER_PREFIX - Active players: $totalActive")
        logger.info(s"$LOGGER_PREFIX - Eligible players: $totalEligible")
        logger.info(
          s"$LOGGER_PREFIX - Top player: ${topPlayer.map(p => s"${p.tag} (${p.combinedRating})").getOrElse("None")}"
        )
      } else {
        logger.warn(s"$LOGGER_PREFIX No eligible players found for good players list")
      }

    } catch {
      case e: Exception =>
        logger.error(s"$LOGGER_PREFIX Failed to recalculate good players", e)
        throw e
    }
  }

  /** Calculate and save top 1000 without cleanup (preserves all existing data) */
  def recalculateGoodPlayersOnly(): Unit = {
    logger.info(s"$LOGGER_PREFIX Starting recalculation of good players (no cleanup)")

    try {
      // Load existing player data
      val allPlayers = loadAllPlayers()

      if (allPlayers.isEmpty) {
        logger.warn(s"$LOGGER_PREFIX No existing player data found")
        return
      }

      // Generate good players summary without cleanup
      val goodPlayers = generateGoodPlayersSummary(allPlayers)

      if (goodPlayers.nonEmpty) {
        import spark.implicits._
        import scala3encoders.given

        // Save only the good players file (don't touch all_players)
        val oldGoodPlayersFile = Paths.get(goodPlayersFile)
        if (Files.exists(oldGoodPlayersFile)) {
          val ts       = timestampFmt.format(Instant.now())
          val archived = Paths.get(s"$goodPlayersDir/good_players_$ts.json")
          Files.move(oldGoodPlayersFile, archived, StandardCopyOption.REPLACE_EXISTING)
          logger.info(s"Archived old good_players file to $archived")
        }

        // Save new good players
        val goodPlayersDF = spark.createDataset(goodPlayers).toDF()
        goodPlayersDF
          .coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .json(goodPlayersFile)

        logger.info(s"$LOGGER_PREFIX Successfully updated ${goodPlayers.length} good players")

        // Log top 10 for verification
        goodPlayers.take(10).zipWithIndex.foreach { case (player, idx) =>
          logger.info(
            s"$LOGGER_PREFIX Rank ${idx + 1}: ${player.tag} - Rating: ${player.combinedRating} - Battles: ${player.battles}"
          )
        }
      } else {
        logger.warn(s"$LOGGER_PREFIX No eligible players found for good players list")
      }

    } catch {
      case e: Exception =>
        logger.error(s"$LOGGER_PREFIX Failed to recalculate good players", e)
        throw e
    }
  }

  /** Get current statistics without modifying files */
  def getPlayerStatistics: Map[String, Any] = {
    logger.info(s"$LOGGER_PREFIX Calculating player statistics")

    try {
      val allPlayers = loadAllPlayers()

      if (allPlayers.isEmpty) {
        return Map("error" -> "No player data found")
      }

      val totalPlayers    = allPlayers.size
      val activePlayers   = allPlayers.count(_._2.isActive)
      val eligiblePlayers = allPlayers.count { case (_, profile) =>
        profile.isActive
      }

      val totalBattles        = allPlayers.values.map(_.battles).sum
      val avgBattlesPerPlayer = if (totalPlayers > 0) totalBattles.toDouble / totalPlayers else 0.0

      val ratingStats  = allPlayers.values.map(_.combinedRating).toSeq.sorted
      val medianRating = if (ratingStats.nonEmpty) {
        val mid = ratingStats.length / 2
        if (ratingStats.length % 2 == 0) {
          (ratingStats(mid - 1) + ratingStats(mid)) / 2.0
        } else {
          ratingStats(mid)
        }
      } else 0.0

      val topPlayers = allPlayers.values.toSeq
        .sortBy(_.combinedRating)(Ordering[Double].reverse)
        .take(10)

      val stats = Map(
        "totalPlayers"        -> totalPlayers,
        "activePlayers"       -> activePlayers,
        "eligiblePlayers"     -> eligiblePlayers,
        "totalBattles"        -> totalBattles,
        "avgBattlesPerPlayer" -> avgBattlesPerPlayer,
        "medianRating"        -> medianRating,
        "topPlayers"          -> topPlayers.map(p =>
          Map(
            "tag"     -> p.tag,
            "rating"  -> p.combinedRating,
            "battles" -> p.battles,
            "winRate" -> (if (p.battles > 0) p.wins.toDouble / p.battles else 0.0)
          )
        )
      )

      logger.info(
        s"$LOGGER_PREFIX Statistics calculated: $totalPlayers total, $activePlayers active, $eligiblePlayers eligible"
      )
      stats

    } catch {
      case e: Exception =>
        logger.error(s"$LOGGER_PREFIX Failed to calculate statistics", e)
        Map("error" -> e.getMessage)
    }
  }

  /** Streaming query */
  def run(): Unit =
    try {
      val silverStream = spark.readStream
        .schema(spark.read.parquet("data/silver").schema) // use static schema
        .option("maxFilesPerTrigger", 1)                  // simulate streaming
        .parquet("data/silver")

      val query = silverStream.writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          try {
            logger.info(s"$LOGGER_PREFIX Processing batch $batchId with ${batchDF.count()} records")

            computeGoodPlayers(batchDF)

            logger.info(s"$LOGGER_PREFIX Completed processing batch $batchId")
          } catch {
            case e: Exception =>
              logger.error(s"$LOGGER_PREFIX Failed to process batch $batchId", e)
            // Continue processing next batch
          }
        }
        .outputMode("append")
        .start()

      query.awaitTermination()
    } catch {
      case e: Exception =>
        logger.error(s"$LOGGER_PREFIX Fatal error in GPA stream", e)
        throw e
    } finally
      if (spark != null) {
        spark.stop()
        logger.info(s"$LOGGER_PREFIX Spark session stopped")
      }
}
