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
          if (Set("producer", "consumerBronze", "consumerSilver", "consumerGold", "GpaInit", "GpaUpdate").contains(m))
            success
          else
            failure("Mode must be one of: producer, consumerBronze, consumerSilver, consumerGold, GpaInit, GpaUpdate")
        )
        .action((x, c) => c.copy(mode = x))
        .text("Mode to run: producer | consumerBronze | consumerSilver | consumerGold | GpaInit | GpaUpdate"),

      // Brawl Stars API token (required if producer)
      opt[Option[String]]('t', "bsToken")
        .action((x, c) => c.copy(bsToken = x))
        .text("Brawl Stars API token")
    )
  }
}
