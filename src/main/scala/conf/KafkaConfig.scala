package conf

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.util.Properties

/** Holds Kafka configuration settings.
  */
// Kafka configuration case class
case class KafkaConfig(
  bootstrapServers: String,                                        // Kafka broker(s) address
  topicProduceTo: Option[String],                                  // Target topic for messages (None if consumer only)
  topicConsumeFrom: Option[String],                                // Source topic for messages (None if producer only)
  acks: String = "all",                                            // Acknowledgment mode
  retries: Int = 3,                                                // Number of retries on failure
  lingerMs: Int = 1,                                               // Time to buffer data before sending
  keySerializer: String = classOf[StringSerializer].getName,       // Key serializer class
  valueSerializer: String = classOf[StringSerializer].getName,     // Value serializer class
  keyDeserializer: String = classOf[StringDeserializer].getName,   // Key deserializer class
  valueDeserializer: String = classOf[StringDeserializer].getName, // Value deserializer class
  groupId: Option[String]
) {
  // Convert KafkaConfig to Properties for KafkaProducer
  def toProducerProperties: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)
    props.put(ProducerConfig.ACKS_CONFIG, acks)
    props.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
    props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs.toString)
    props
  }

  // Convert KafkaConfig to Properties for KafkaConsumer
  def toConsumerProperties: Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId.getOrElse("unknown-group"))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props
  }
}
