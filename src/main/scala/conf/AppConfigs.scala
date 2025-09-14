package conf

import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*

/** Holds all configuration values for the application (both defaults and user-set).
  */

case class AppConfig(
  mode: String,            // Mode to run: producer | consumerBronze | consumerSilver | consumerGold
  bsToken: Option[String], // Brawl Stars API token
  goodPlayersFile: String, // File path that contains good player tags and other metadata
  kafka: KafkaConfig       // Kafka configuration settings
) derives ConfigReader

/** Holds default configuration values for the application. TODO: Add DataConfig here for dynamic file paths
  */
case class AppConfigDefaults(
  goodPlayersFile: String, // File path that contains good player tags and other metadata
  kafka: KafkaConfig       // Kafka configuration settings
) derives ConfigReader
