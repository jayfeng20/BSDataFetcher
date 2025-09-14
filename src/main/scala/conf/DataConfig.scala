package conf

/** Holds data-related configuration settings.
  * @param goodPlayersFile:
  *   File path that contains good player tags and other metadata
  * @param rawBattleLogDir:
  *   Directory to store raw battle logs
  * @param BattleLogParquetDir:
  *   Directory to store processed battle logs in Parquet format
  */
case class DataConfig(
  goodPlayersFile: String,    // File path that contains good player tags and other metadata
  rawBattleLogDir: String,    // Directory to store raw battle logs
  BattleLogParquetDir: String // Directory to store processed battle logs in Parquet format
)
