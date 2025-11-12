import brawlstars.api
import com.typesafe.scalalogging.Logger
import conf.{AppConfig, AppConfigDefaults, CliArgs, KafkaConfig, ParserBuilder}
import conf.ParserBuilder.*
import pipeline.gpa.GPA
import scopt.OParser
import pipeline.{BronzeConsumer, Producer, SilverConsumer}
import pureconfig.*
import pureconfig.error.*
//import pureconfig.module.catseffect.syntax._
import pureconfig.ConfigSource

/** Modes to run this application
  */
sealed trait Mode

object Mode {
  case object Producer                     extends Mode
  case object ConsumerBronze               extends Mode
  case object ConsumerSilver               extends Mode
  case object GpaInit                      extends Mode
  case object GpaUpdate                    extends Mode
  case class UnknownMode(errorMsg: String) extends Throwable

  // parse a mode string from cli as a type-safe Mode
  def parse(modeStr: String): Either[UnknownMode, Mode] =
    modeStr.toLowerCase match {
      case "producer"       => Right(Producer)
      case "consumerbronze" => Right(ConsumerBronze)
      case "consumersilver" => Right(ConsumerSilver)
      case "gpainit"        => Right(GpaInit)
      case "gpaupdate"      => Right(GpaUpdate)
      case unknownMode      => Left(UnknownMode(unknownMode))
    }

}

object Main {
  def main(args: Array[String]): Unit = {

    val logger = Logger("BSDataFetcher")

    val config = loadConfig(args) match {
      case Right(c: AppConfig) => c
      case Left(error)         =>
        logger.error(s"Configuration error: $error")
        sys.exit(1)
    }

    logger.info(s"Running mode: ${config.mode}")

    Mode.parse(config.mode) match {
      case Right(Mode.Producer) =>
        if config.bsToken.isEmpty then
          logger.error("Brawl Stars API token is required in producer mode")
          sys.exit(1)
        val producer = new Producer(config)
        producer.sendRawGoodPlayerBattleLogs()
      case Right(Mode.ConsumerBronze) =>
        val consumer = new BronzeConsumer(config)
        consumer.run()
      case Right(Mode.ConsumerSilver) =>
        val consumer = new SilverConsumer(config)
        consumer.run()
      case Right(Mode.GpaInit) =>
        val gpa = new GPA(config)
        gpa.generateInitialGoodPlayersFromSeeds()
      case Right(Mode.GpaUpdate) =>
        val gpa = new GPA(config)
        gpa.run()
      case Left(unknownMode) =>
        logger.error(s"Mode not implemented: ${unknownMode.errorMsg}")
    }
  }

  private def loadConfig(args: Array[String]): Either[String, AppConfig] =
    OParser.parse(ParserBuilder.parser, args, CliArgs()) match {
      case Some(cliArgs) if cliArgs.mode.nonEmpty =>
        ConfigSource.default.at("app").load[AppConfigDefaults] match {
          case Right(defaults: AppConfigDefaults) =>
            val kafkaConf: KafkaConfig = defaults.kafka.copy(
              bootstrapServers = cliArgs.bootstrapServers.getOrElse(defaults.kafka.bootstrapServers),
              topicProduceTo = cliArgs.mode match {
                case "producer" => Some("battlelog-raw-topic")
                case other      => None
              },
              topicConsumeFrom = cliArgs.mode match {
                case "consumerBronze" => Some("battlelog-raw-topic")
                case "consumerSilver" => Some("battlelog-raw-topic")
                case other            => None
              },
              groupId = cliArgs.mode match {
                case "consumerBronze" => Some("bronze-group")
                case "consumerSilver" => Some("silver-group")
                case other            => None
              }
            )
            Right(
              AppConfig(
                mode = cliArgs.mode,
                bsToken = cliArgs.bsToken,
                goodPlayersFile = defaults.goodPlayersFile,
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
