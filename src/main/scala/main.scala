import com.typesafe.scalalogging.Logger

@main
def main(): Unit = {
  val logger = Logger("BSDataFetcher")
  logger.info("Starting Brawl Stars Data Fetcher")

  // Make sure a brawl stars api token is set in environment variables
  val api_token = sys.env.get("BRAWL_STARS_API_TOKEN_THEDEAN") match {
    case Some(token) => token
    case None =>
      logger.error("No brawl stars api token is found")
      sys.exit(1)
  }
}


