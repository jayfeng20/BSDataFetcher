package conf

import scopt.{OParser, OParserBuilder}

object ParserBuilder {

  private val builder: OParserBuilder[CliArgs] = OParser.builder[CliArgs]

  val parser: OParser[Unit, CliArgs] = {
    import builder._

    OParser.sequence(
      programName("BSDataFetcher"),
      head("BSDataFetcher", "0.1"),

      // Mode argument (required)
      opt[String]('m', "mode")
        .required()
        .validate(m =>
          if (Set("producer", "consumerBronze", "consumerSilver", "consumerGold").contains(m)) success
          else failure("Mode must be one of: producer, consumerBronze, consumerSilver, consumerGold")
        )
        .action((x, c) => c.copy(mode = x))
        .text("Mode to run: producer | consumerBronze | consumerSilver | consumerGold"),

      // Brawl Stars API token (required if producer)
      opt[Option[String]]('t', "bsToken")
        .action((x, c) => c.copy(bsToken = x))
        .text("Brawl Stars API token"),

      // File containing initial good player tags (optional with default)
      opt[String]('f', "goodPlayersFile")
        .action((x, c) => c.copy(goodPlayersFile = x))
        .text(
          "File path with initial good player tags (default: data/good_players/good_players_0000-00-00T00-00-00.json)"
        )
    )
  }
}
