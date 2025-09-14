package conf

case class CliArgs(
  mode: String = "",
  bsToken: Option[String] = None,
  bootstrapServers: Option[String] = None
)
