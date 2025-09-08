import brawlstars.api
import com.typesafe.scalalogging.Logger
import conf.{AppConfig, AppConfigDefaults, CliArgs, KafkaConfig, ParserBuilder}
import conf.ParserBuilder.*
import scopt.OParser
import pipeline.Producer
import pureconfig._
import pureconfig.error._
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
            val producer = new Producer(config)
            producer.sendGoodPlayers()
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
      case Some(cliArgs) if cliArgs.mode.nonEmpty && cliArgs.bsToken.nonEmpty =>
        ConfigSource.default.at("app").load[AppConfigDefaults] match {
          case Right(defaults: AppConfigDefaults) =>
            val kafkaConf = defaults.kafka.copy(
              bootstrapServers = cliArgs.bootstrapServers.getOrElse(defaults.kafka.bootstrapServers)
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
        Left("Missing required arguments: mode and bsToken are required")
      case None =>
        Left("Invalid command-line arguments (use --help for usage)")
    }

}
