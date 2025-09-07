package pipeline

import java.util.Properties
import scala.io.Source
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import com.typesafe.scalalogging.Logger
import io.circe.*
import io.circe.parser.*
import io.circe.generic.auto.*
import io.circe.syntax.*

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Using

case class GoodPlayer(tag: String, lastSeen: String, rating: Int)

class Producer(bsToken: String, goodPlayersFile: String) {

  private val logger = Logger[this.type]

//  private val topic = "battlelog-raw"
//
//  // Kafka configuration
//  private val props = new Properties()
//  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//
//  private val producer = new KafkaProducer[String, String](props)

  // Load good players from JSON using Circe
  def loadGoodPlayers(): List[String] = {
    val jsonStr = Using(Source.fromFile(goodPlayersFile)) { source =>
      source.getLines().mkString
    }.getOrElse {
      // fallback if file read fails
      logger.error(s"Failed to read $goodPlayersFile")
      ""
    }
    decode[List[GoodPlayer]](jsonStr) match {
      case Right(players) => players.map(_.tag)
      case Left(error)    =>
        logger.error(s"Failed to parse JSON: $error")
        List.empty[String]
    }
  }

  // Simulate fetching battlelog from API
  private def fetchBattlelog(tag: String): String =
    s"""{"playerTag": "$tag", "battle": "sampleData"}"""

//  def run(): Unit = {
//    val players = loadGoodPlayers()
//    logger.info(s"Producer: Found ${players.size} players to fetch")
//
//    players.foreach { tag =>
//      val battlelogJson = fetchBattlelog(tag)
//      val record = new ProducerRecord[String, String](topic, tag, battlelogJson)
//      producer.send(record)
//      logger.info(s"Produced battlelog for $tag")
//      Thread.sleep(100) // throttle for API rate limits
//    }
//
//    producer.close()
//    logger.info("Producer finished producing battlelogs")
//  }
}
