package kafka.practice.consumers;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kafka.practice.constants.Constant.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_VALUE);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_VALUE);
        properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "52428800");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        properties.setProperty(SECURITY_PROTOCOL_KEY, SECURITY_PROTOCOL_VALUE);
        properties.setProperty(SASL_MECHANISM_KEY, SASL_MECHANISM_VALUE);
        properties.setProperty(SASL_JAAS_CONFIG_KEY, SASL_JAAS_CONFIG_VALUE);

        // create kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to topic(s)
        consumer.subscribe(Collections.singleton(TOPIC_VALUE));

        // poll records
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord consumerRecord : consumerRecords) {
                LOG.info("Key = " + consumerRecord.key());
                LOG.info("Value = " + consumerRecord.value());
                LOG.info("Topic = " + consumerRecord.topic());
                LOG.info("Partition = " + consumerRecord.partition());
                LOG.info("Offset = " + consumerRecord.offset());
                LOG.info("Timestamp = " + consumerRecord.timestamp());

                LOG.info("\n");
            }
        }
    }
}
