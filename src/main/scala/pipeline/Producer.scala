package pipeline
import brawlstars.model.BattleLogModel.BattleLogResponse
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import io.circe.generic.auto.*
import io.circe.syntax.*

import scala.io.Source
import java.util.Properties
import scala.util.{Failure, Success, Try}
import io.circe.parser.decode
import io.circe.generic.auto.*
import model.GoodPlayerSummary
import conf.AppConfig
import io.circe.{Decoder, Encoder}

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger

/** Producer class to read good players from a file, fetch their battle logs, and send the raw battle logs to a Kafka
  * topic.
  * @param config:
  *   Application configuration containing Kafka settings and file paths
  */
class Producer(config: AppConfig) {
  private val logger = Logger[this.type]

  private def createProducer: KafkaProducer[String, String] =
    new KafkaProducer[String, String](config.kafka.toProducerProperties)

  private case class PlayerTag(tag: String)

  /** Reads good players from a JSON file.
    *
    * @param dirPath
    *   : Path to the JSON file containing good player data
    * @return
    *   List[String]: List of good player tags
    */
  private def readGoodPlayers(dirPath: String): List[String] = {
    val jsonFiles = new File(dirPath).listFiles().filter(f => f.isFile && f.getName.endsWith(".json"))

    if (jsonFiles.isEmpty) {
      logger.warn(s"No JSON files found in directory: $dirPath")
      return List.empty
    }

    jsonFiles.flatMap { file =>
      Try {
        val source = Source.fromFile(file)
        try
          source
            .getLines()
            .flatMap { line =>
              decode[PlayerTag](line) match {
                case Right(player) => Some(player.tag)
                case Left(err)     =>
                  logger.error(s"Failed to parse line in ${file.getAbsolutePath}: $err")
                  None
              }
            }
            .toList
        finally source.close()
      } match {
        case Success(tags) => tags
        case Failure(ex)   =>
          logger.error(s"Failed to read file: ${file.getAbsolutePath}", ex)
          List.empty[String]
      }
    }.toList
  }

  //  private def readGoodPlayers(filePath: String): List[GoodPlayerSummary] =
//    Try {
//      val source  = Source.fromFile(filePath)
//      val content =
//        try source.mkString
//        finally source.close()
//      decode[List[GoodPlayerSummary]](content) match {
//        case Right(players) => players
//        case Left(error)    =>
//          logger.error(s"Failed to parse GoodPlayers from file: $filePath. Error: $error")
//          List.empty[GoodPlayerSummary]
//      }
//    } match {
//      case Success(players)   => players
//      case Failure(exception) =>
//        logger.error(s"Failed to read good players file: $filePath", exception)
//        List.empty[GoodPlayerSummary]
//    }

  /** Fetches battle logs for a list of good players.
    * @param goodPlayerTags:
    *   List of good player Tags
    * @return
    *   Either an error message or a list of battle logs
    */
  private def getGoodPlayerBattleLogs(goodPlayerTags: List[String]): List[Either[String, BattleLogResponse]] = {
    val bsToken  = config.bsToken.get
    val bsClient = new brawlstars.api.BrawlStarsClient(bsToken)
    goodPlayerTags.map { tag =>
      bsClient.fetchBattleLog(tag)
    }
  }

  /** Sends raw battle logs of good players to a Kafka topic.
    */
  def sendRawGoodPlayerBattleLogs(): Unit = {
    // Create Kafka producer
    val producer = createProducer

    // Atomic counter to track successful sends for logging purposes
    val successCounter = new AtomicInteger(0)

    // only read from init file if primary file doesn't exist
    // TODO: make this an app level config
    val goodPlayersFile =
      if Files.exists(Paths.get(config.goodPlayersFile)) then config.goodPlayersFile
      else "data/good_players/good_players_init.json"

    val players                      = readGoodPlayers(goodPlayersFile)
    val maybeRawGoodPlayerBattleLogs = getGoodPlayerBattleLogs(players)

    // Combine players' tags with their corresponding battle logs or errors
    val battleLogsWithTags = players
      .zip(maybeRawGoodPlayerBattleLogs)

    try
      battleLogsWithTags.foreach {
        case (tag, Right(battleLog)) =>
          val key    = tag
          val value  = battleLog.asJson.noSpaces
          val record = new ProducerRecord[String, String](config.kafka.topicProduceTo.get, key, value)
          producer.send(
            record,
            (metadata, exception) =>
              if (exception != null) {
                logger.error(s"Failed to send record for player $tag", exception)
              } else {
                logger.info(
                  s"Sent record for player $tag to topic ${metadata.topic()}" +
                    s" partition ${metadata.partition()} offset ${metadata.offset()}"
                )
                successCounter.incrementAndGet()
              }
          )
        // don't fail entire batch if one record fails
        case (tag, Left(error)) =>
          logger.warn(s"Failed to fetch battle log for player $tag: $error")
      }
    finally {
      producer.flush()
      logger.info(
        s"Out of ${players.length} players, successfully sent ${successCounter.get()} player records to topic ${config.kafka.topicProduceTo.get}"
      )
      producer.close()
    }
  }
}

implicit val timestampDecoder: Decoder[Timestamp] =
  Decoder.decodeString.emap { str =>
    try
      Right(Timestamp.valueOf(str))
    catch {
      case _: IllegalArgumentException =>
        Left(s"Invalid timestamp: $str")
    }
  }

implicit val timestampEncoder: Encoder[Timestamp] =
  Encoder.encodeString.contramap[Timestamp](_.toString)
