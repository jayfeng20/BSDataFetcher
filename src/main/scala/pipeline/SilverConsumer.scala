package pipeline

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.functions.{col, lit, to_date}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer

import scala.jdk.CollectionConverters.*
import io.circe.parser.decode
import io.circe.generic.auto.*

import java.time.Instant
import java.security.MessageDigest
import java.util.Properties
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.ZoneOffset
import java.io.File
import java.sql.Timestamp
import conf.AppConfig
import brawlstars.model.BattleLogModel.{BattleLogResponse, Brawler, Player}
import brawlstars.model.{GameResult, GameType}

import java.time.temporal.ChronoField

/** Case class representing the schema of the Parquet data, which needs to be compatible with Spark DataFrame
  * transformations (especially we're using Scala3 which Spark does not officially support, so we're using basic types
  * where possible
  */
case class BattleMatch(
  matchId: String,
  battleTime: java.sql.Timestamp,
  mode: String,
  map: String,
  `type`: String,
  result: String,
  duration: Int,
  starPlayer: Player,
  teams: Seq[Seq[Player]],
  raw: String
)

/** SilverConsumer:
  *   - Consumes raw battle logs from Kafka (raw-battlelog-topic)
  *   - Adds matchId for deduplication (battleTime + sorted players)
  *   - Writes full JSON structure into Parquet under data/silver
  *   - Publishes deduped messages to parquet-battlelog-topic
  */
class SilverConsumer(config: AppConfig) {
  private val LOGGER_PREFIX = "[SilverConsumer]"
  private val logger        = Logger(this.getClass)

  private val consumer = new KafkaConsumer[String, String](config.kafka.toConsumerProperties)
  consumer.subscribe(java.util.Arrays.asList(config.kafka.topicConsumeFrom.get))

  private val producer = new KafkaProducer[String, String](config.kafka.toProducerProperties)

  private val baseDir = new File("data/silver")
  if (!baseDir.exists()) baseDir.mkdirs()

  private val spark: SparkSession = SparkSession
    .builder()
    .appName("SilverConsumer")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import scala3encoders.given // for automatic derivation of encoders in Scala 3

  private def sha256(s: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    md.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  val battleTimeFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendPattern("yyyyMMdd'T'HHmmss.SSS'Z'")
    .parseDefaulting(ChronoField.OFFSET_SECONDS, 0)
    .toFormatter()

  def run(): Unit =
    while (true) {
      val records = consumer.poll(java.time.Duration.ofMillis(1000))

      if (!records.isEmpty) {
        logger.info(s"$LOGGER_PREFIX Consumed ${records.count()} raw records")

        // Parse records safely
        val parsedMatches = records.asScala.flatMap(parseRecord).toSeq

        if (parsedMatches.nonEmpty) {
          logger.info(
            s"$LOGGER_PREFIX Received ${records.count()} records, parsed ${parsedMatches.size} valid matches."
          )

          // deduplicate based on matchId
          // Make sure no duplicates exist in both current batch and historical data

          val df          = createDataFrame(parsedMatches)
          val dfWithDate  = df.withColumn("battleDate", to_date($"battleTime"))
          val battleDates = dfWithDate.select("battleDate").distinct().as[String].collect()

          // read historical data (only relevant partitions)
          val existingPaths = battleDates.map(d => s"data/silver/battleDate=$d").filter(p => new java.io.File(p).exists)
          val existingDF = if (existingPaths.nonEmpty) spark.read.parquet(existingPaths: _*) else spark.emptyDataFrame

          // Combine, deduplicate, and overwrite
          val combinedDF =
            if !existingDF.isEmpty then existingDF.union(dfWithDate)
            else dfWithDate
          val dedupedDF = combinedDF.dropDuplicates("matchId")

          try {
            // Write to Silver layer
            dedupedDF.write
              .mode(SaveMode.Append)
              .partitionBy("battleDate")
              .parquet("data/silver")

            // Commit Kafka offsets only after successful write
            consumer.commitSync()
            logger.info(s"$LOGGER_PREFIX Wrote ${df.count()} deduped matches to Silver layer and committed offsets.")
          } catch {
            case e: Exception =>
              logger.error(s"$LOGGER_PREFIX Failed to write batch or commit offsets", e)
            // offsets not committed, batch can be retried
          }
        } else {
          logger.info(s"$LOGGER_PREFIX No valid battle logs in this batch.")
        }
      }
    }

  /** Parse a single Kafka record into BattleMatch safely */
  private def parseRecord(record: ConsumerRecord[String, String]): Seq[BattleMatch] =
    decode[BattleLogResponse](record.value()) match {
      case Right(battleLogResp) =>
        battleLogResp.items.map { battle =>
          val tags    = battle.battle.teams.getOrElse(Seq.empty).flatten.map(_.tag).sorted
          val matchId = sha256(battle.battleTime + tags.mkString(","))

          BattleMatch(
            matchId = matchId,
            battleTime = safeParseTimestamp(battle.battleTime),
            mode = battle.event.mode.getOrElse("unknown"),
            map = battle.event.map.getOrElse("unknown"),
            `type` = battle.battle.`type`.toString,
            result = battle.battle.result.toString,
            duration = battle.battle.duration.getOrElse(-1),
            starPlayer = battle.battle.starPlayer.getOrElse(defaultPlayer),
            teams = battle.battle.teams.getOrElse(Seq.empty),
            raw = record.value()
          )
        }
      case Left(err) =>
        logger.warn(s"$LOGGER_PREFIX Failed to parse record: $err")
        Nil
    }

  private val defaultPlayer = Player("unknown", "unknown", Brawler(-1, "unknown", -1, -1))

  private def safeParseTimestamp(ts: String): Timestamp =
    try Timestamp.from(Instant.from(battleTimeFormatter.parse(ts)))
    catch {
      case _: Exception =>
        logger.warn(s"$LOGGER_PREFIX Failed to parse timestamp '$ts', using epoch")
        new Timestamp(0)
    }

  /** Convert parsed Seq[BattleMatch] to DataFrame */
  private def createDataFrame(matches: Seq[BattleMatch]) = {
    import spark.implicits.*
    import scala3encoders.given
    matches.toDF()
  }
}
