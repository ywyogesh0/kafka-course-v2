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

        String groupIdValue = "g1";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIdValue);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_VALUE);

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
