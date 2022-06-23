package kafka.practice.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

import static kafka.practice.constants.Constant.*;

public class ProducerDemoWithCallback {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_VALUE);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_VALUE);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_VALUE);
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS_VALUE);
        properties.setProperty(SECURITY_PROTOCOL_KEY, SECURITY_PROTOCOL_VALUE);
        properties.setProperty(SASL_MECHANISM_KEY, SASL_MECHANISM_VALUE);
        properties.setProperty(SASL_JAAS_CONFIG_KEY, SASL_JAAS_CONFIG_VALUE);

        // create kafka producer
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(TOPIC_VALUE, "Today is 24th April, 22 -> " + Instant.now().toEpochMilli());

        // send records - asynchronous call
        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                LOG.info("RecordMetadata : \n" +
                        "Topic : " + recordMetadata.topic() + "\n" +
                        "Partition : " + recordMetadata.partition() + "\n" +
                        "Offset : " + recordMetadata.offset() + "\n" +
                        "Timestamp : " + recordMetadata.timestamp());

                LOG.info("\n");
            } else {
                LOG.error("Error While Producing", e);
            }
        });

        // flush and close
        producer.close();
    }
}
