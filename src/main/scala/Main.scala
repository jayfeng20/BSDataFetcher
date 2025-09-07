import brawlstars.api
import com.typesafe.scalalogging.Logger
import config.ParserBuilder._
import scopt.OParser
import config.CliArgs
import pipeline.Producer

object Main {
  def main(args: Array[String]): Unit = {
    val logger = Logger("BSDataFetcher")

    // Command line argument parsing
    OParser.parse(parser, args, CliArgs()) match {
      case Some(config) =>
        config.mode match {
          case "producer" =>
            val producer    = new Producer(config.bsToken, config.goodPlayersFile)
            val goodPlayers = producer.loadGoodPlayers()
            println(s"Loaded ${goodPlayers.length} good players:")
          //          producer.run()
          case "consumerA" => println("ConsumerA not implemented yet")
          case "consumerB" => println("ConsumerB not implemented yet")
          case "consumerC" => println("ConsumerC not implemented yet")
        }

      case None =>
        // arguments are bad, error message will be displayed by scopt
        sys.exit(1)
    }

//    logger.info("Starting BSDataFetcher")
//
//    // Make sure a brawl stars api token is set in environment variables
//    val api_token = sys.env.get("BRAWL_STARS_API_TOKEN_THEDEAN") match {
//      case Some(token) => token
//      case None =>
//        logger.error("No brawl stars api token is found")
//        sys.exit(1)
//    }
//
//    val client = new api.BrawlStarsClient(api_token)
//    val playerTag = "#2QQRLCGYVR"
//    client.fetchBattleLog(playerTag) match {
//      case Right(battleLog) =>
//        logger.info(s"Fetched ${battleLog.items.length} battles for player $playerTag")
//        battleLog.items.foreach { battle =>
//          logger.info(s"Battle at ${battle.battleTime}: ${battle.battle.mode} - ${battle.battle.result}")
//        }
//      case Left(error) =>
//        logger.error(error)
//    }
  }
}
