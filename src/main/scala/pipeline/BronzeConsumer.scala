package pipeline

import com.typesafe.scalalogging.Logger
import conf.AppConfig
import java.io.{BufferedWriter, File, FileWriter}
import java.time.{Instant, LocalDate, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.*
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._

/** Bronze Layer BronzeConsumer reads raw battle log messages from a Kafka topic and writes them to JSONL files in the
  * "data/bronze as default"
  *
  * @param config
  *   : Application configuration containing Kafka settings and file paths
  */
class BronzeConsumer(config: AppConfig) {

  private val LOGGER_PREFIX = "[BronzeConsumer]"
  private val logger        = Logger(this.getClass)

  // Kafka consumer
  private val consumer = new KafkaConsumer[String, String](config.kafka.toConsumerProperties)
  consumer.subscribe(java.util.Arrays.asList(config.kafka.topicConsumeFrom.get))

  // Base output directory to write raw battelog JSONL files
  private val baseDir = new File("data/bronze")
  if (!baseDir.exists()) baseDir.mkdirs()

  // Timestamp formatter for filenames
  private val timestampFormatter = DateTimeFormatter
    .ofPattern("yyyyMMdd_HHmmss_SSS")
    .withZone(ZoneOffset.UTC)

  def run(): Unit = {
    logger.info(s"$LOGGER_PREFIX Writing raw logs to ${baseDir.getAbsolutePath}")

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(1000))

      if (!records.isEmpty) {
        val now       = Instant.now()
        val localDate = LocalDate.now(ZoneOffset.UTC)

        // Partition directory by date
        val dir = new File(
          baseDir,
          s"year=${localDate.getYear}/month=${"%02d".format(localDate.getMonthValue)}/day=${"%02d".format(localDate.getDayOfMonth)}"
        )
        if (!dir.exists()) dir.mkdirs()

        // Filename with timestamp
        val batchTimestamp = timestampFormatter.format(now)
        val file           = new File(dir, s"battlelog_$batchTimestamp.jsonl")
        val writer         = new BufferedWriter(new FileWriter(file, true))
        val successCounter = new AtomicInteger(0)

        try
          for (record <- records.asScala) {
            // Wrap raw message with Kafka key
            val jsonWrapped: Json = parse(record.value()) match {
              case Right(parsed) =>
                Json.obj(
                  "tag"       -> Json.fromString(record.key()),
                  "battleLog" -> parsed
                )
              case Left(_) =>
                Json.obj(
                  "tag"          -> Json.fromString(record.key()),
                  "battleLogRaw" -> Json.fromString(record.value())
                )
            }

            writer.write(jsonWrapped.noSpaces)
            writer.newLine()
            successCounter.incrementAndGet()
          }
        finally
          writer.close()

        logger.info(s"$LOGGER_PREFIX Wrote ${successCounter.get()} records to ${file.getAbsolutePath}")
      }
    }
  }
}
