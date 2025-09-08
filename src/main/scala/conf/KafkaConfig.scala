package conf

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

/** Holds Kafka configuration settings.
  */
// Kafka configuration case class
case class KafkaConfig(
  bootstrapServers: String,                                   // Kafka broker(s) address
  topic: String,                                              // Target topic for messages
  acks: String = "all",                                       // Acknowledgment mode
  retries: Int = 3,                                           // Number of retries on failure
  lingerMs: Int = 10,                                         // Time to buffer data before sending
  keySerializer: String = classOf[StringSerializer].getName,  // Key serializer class
  valueSerializer: String = classOf[StringSerializer].getName // Value serializer class
) {
  // Convert KafkaConfig to Properties for KafkaProducer
  def toProperties: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)
    props.put(ProducerConfig.ACKS_CONFIG, acks)
    props.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
    props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs.toString)
    props
  }
}
