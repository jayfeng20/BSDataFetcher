import brawlstars.api
import com.typesafe.scalalogging.Logger
import conf.{AppConfig, AppConfigDefaults, CliArgs, KafkaConfig, ParserBuilder}
import conf.ParserBuilder.*
import scopt.OParser
import pipeline.{BronzeConsumer, Producer}
import pureconfig.*
import pureconfig.error.*
//import pureconfig.module.catseffect.syntax._
import pureconfig.ConfigSource

object Main {
  def main(args: Array[String]): Unit = {

    val logger = Logger("BSDataFetcher")

    loadConfig(args) match {
      case Right(config: AppConfig) =>
        logger.info(s"Running mode: ${config.mode}")
        config.mode match {
          case "producer" =>
            if config.bsToken.isEmpty then
              logger.error("Brawl Stars API token is required in producer mode")
              sys.exit(1)
            val producer = new Producer(config)
            producer.sendRawGoodPlayerBattleLogs()
          case "consumerBronze" =>
            val consumer = new BronzeConsumer(config)
            consumer.run()
          case "consumerSilver" =>
            val consumer = new pipeline.SilverConsumer(config)
            consumer.run()
          case other =>
            logger.error(s"Mode not implemented: $other")
        }
      case Left(error) =>
        logger.error(s"Configuration error: $error")
        sys.exit(1)
    }
  }

  private def loadConfig(args: Array[String]): Either[String, AppConfig] =
    OParser.parse(ParserBuilder.parser, args, CliArgs()) match {
      case Some(cliArgs) if cliArgs.mode.nonEmpty =>
        ConfigSource.default.at("app").load[AppConfigDefaults] match {
          case Right(defaults: AppConfigDefaults) =>
            val kafkaConf = defaults.kafka.copy(
              bootstrapServers = cliArgs.bootstrapServers.getOrElse(defaults.kafka.bootstrapServers),
              topicProduceTo = cliArgs.mode match {
                case "consumerBronze" => None
                case "consumerSilver" => None
                case "producer"       => Some("battlelog-raw-topic")
              },
              topicConsumeFrom = cliArgs.mode match {
                case "consumerBronze" => Some("battlelog-raw-topic")
                case "consumerSilver" => Some("battlelog-raw-topic")
                case "producer"       => None
              },
              groupId = cliArgs.mode match {
                case "consumerBronze" => Some("bronze-group")
                case "consumerSilver" => Some("silver-group")
                case "producer"       => None
              }
            )
            Right(
              AppConfig(
                mode = cliArgs.mode,
                bsToken = cliArgs.bsToken,
                goodPlayersFile =
                  if cliArgs.goodPlayersFile.nonEmpty then cliArgs.goodPlayersFile
                  else defaults.goodPlayersFile,
                kafka = kafkaConf
              )
            )
          case Left(failures) =>
            Left(s"Failed to load application.conf: ${failures.prettyPrint()}")
        }
      case Some(_) =>
        Left("Missing required arguments: mode is missing (use --help for usage)")
      case None =>
        Left("Invalid command-line arguments (use --help for usage)")
    }
}
