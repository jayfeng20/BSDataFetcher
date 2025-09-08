package conf

import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*

/** Holds default configuration values for the application.
  */

case class AppConfig(
  mode: String,            // Mode to run: producer | consumerA | consumerB | consumerC
  bsToken: String,         // Brawl Stars API token
  goodPlayersFile: String, // File path that contains good player tags and other metadata
  kafka: KafkaConfig       // Kafka configuration settings
) derives ConfigReader
