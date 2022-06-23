package kafka.practice.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import static kafka.practice.constants.Constant.*;

import java.util.Properties;

public class ProducerDemo {


    public static void main(String[] args) {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_VALUE);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_VALUE);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_VALUE);
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "300000"); // 5 minutes
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(AUTO_CREATE_TOPICS_ENABLE, AUTO_CREATE_TOPICS_ENABLE_VALUE);
        properties.setProperty(SECURITY_PROTOCOL_KEY, SECURITY_PROTOCOL_VALUE);
        properties.setProperty(SASL_MECHANISM_KEY, SASL_MECHANISM_VALUE);
        properties.setProperty(SASL_JAAS_CONFIG_KEY, SASL_JAAS_CONFIG_VALUE);

        // create kafka producer
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(TOPIC_VALUE, "Today is 23rd Oct, 18");

        // send records - asynchronous call
        producer.send(producerRecord);

        // flush and close
        producer.close();
    }
}
