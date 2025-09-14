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
import model.GoodPlayer
import conf.AppConfig
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

  /** Reads good players from a JSON file.
    * @param filePath:
    *   Path to the JSON file containing good player data
    * @return
    *   List[GoodPlayer]: List of good players
    */
  private def readGoodPlayers(filePath: String): List[GoodPlayer] =
    Try {
      val source  = Source.fromFile(filePath)
      val content =
        try source.mkString
        finally source.close()
      decode[List[GoodPlayer]](content) match {
        case Right(players) => players
        case Left(error)    =>
          logger.error(s"Failed to parse GoodPlayers from file: $filePath. Error: $error")
          List.empty[GoodPlayer]
      }
    } match {
      case Success(players)   => players
      case Failure(exception) =>
        logger.error(s"Failed to read good players file: $filePath", exception)
        List.empty[GoodPlayer]
    }

  /** Fetches battle logs for a list of good players.
    * @param goodPlayers:
    *   List of good players
    * @return
    *   Either an error message or a list of battle logs
    */
  private def getGoodPlayerBattleLogs(goodPlayers: List[GoodPlayer]): List[Either[String, BattleLogResponse]] = {
    val bsToken  = config.bsToken.get
    val bsClient = new brawlstars.api.BrawlStarsClient(bsToken)
    goodPlayers.map { player =>
      bsClient.fetchBattleLog(player.tag)
    }
  }

  /** Sends raw battle logs of good players to a Kafka topic.
    */
  def sendRawGoodPlayerBattleLogs(): Unit = {
    // Create Kafka producer
    val producer = createProducer

    // Atomic counter to track successful sends for logging purposes
    val successCounter = new AtomicInteger(0)

    val players                      = readGoodPlayers(config.goodPlayersFile)
    val maybeRawGoodPlayerBattleLogs = getGoodPlayerBattleLogs(players)

    // Combine players' tags with their corresponding battle logs or errors
    val battleLogsWithTags = players
      .map(_.tag)
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
