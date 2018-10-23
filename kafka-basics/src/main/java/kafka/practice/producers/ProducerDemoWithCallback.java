package kafka.practice.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static kafka.practice.constants.Constant.*;

public class ProducerDemoWithCallback {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

    public static void main(String[] args) {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_VALUE);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_VALUE);

        // create kafka producer
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(TOPIC_VALUE, "Today is 23rd Oct, 18");

        // send records - asynchronus call
        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                System.out.println("RecordMetadata : \n" +
                        "Topic : " + recordMetadata.topic() + "\n" +
                        "Partition : " + recordMetadata.partition() + "\n" +
                        "Offset : " + recordMetadata.offset() + "\n" +
                        "Timestamp : " + recordMetadata.timestamp());

                System.out.println();
            } else {
                LOG.error("Error While Producing", e);
            }
        });

        // flush and close
        producer.close();
    }
}
