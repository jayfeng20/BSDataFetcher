package pipeline
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

class Producer(config: AppConfig) {
  private val logger = Logger[this.type]

  private def createProducer: KafkaProducer[String, String] =
    new KafkaProducer[String, String](config.kafka.toProperties)

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

  def sendGoodPlayers(): Unit = {
    val producer = createProducer
    val players  = readGoodPlayers(config.goodPlayersFile)

    try {
      players.foreach { player =>
        val key    = player.tag
        val value  = player.asJson.noSpaces
        val record = new ProducerRecord[String, String](config.kafka.topic, key, value)
        producer.send(
          record,
          (metadata, exception) =>
            if (exception != null) {
              logger.error(s"Failed to send record for player ${player.tag}", exception)
            } else {
              logger.info(
                s"Sent record for player ${player.tag} to topic ${metadata.topic()}" +
                  s" partition ${metadata.partition()} offset ${metadata.offset()}"
              )
            }
        )
      }
      logger.info(s"Successfully sent ${players.length} player records to topic ${config.kafka.topic}")
    } finally {
      producer.flush()
      producer.close()
    }
  }
}
