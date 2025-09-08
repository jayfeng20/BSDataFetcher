package conf

case class CliArgs(
  mode: String = "",
  bsToken: String = "",
  goodPlayersFile: String = "data/good_players/good_players_0000-00-00T00-00-00.json",
  bootstrapServers: Option[String] = None
)
