package kafka.practice.consumers;

import org.apache.kafka.clients.consumer.*;

import static kafka.practice.constants.Constant.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS_VALUE);

        // create kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to topic(s)
        consumer.subscribe(Collections.singleton(TOPIC_VALUE));

        // poll records
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord consumerRecord : consumerRecords) {
            System.out.println("Key = " + consumerRecord.key() + "\n");
            System.out.println("Value = " + consumerRecord.value() + "\n");
            System.out.println("Topic = " + consumerRecord.topic() + "\n");
            System.out.println("Partition = " + consumerRecord.partition() + "\n");
            System.out.println("Offset = " + consumerRecord.offset() + "\n");
            System.out.println("Timestamp = " + consumerRecord.timestamp() + "\n");
        }

        // close consumer
        consumer.close();
    }
}
