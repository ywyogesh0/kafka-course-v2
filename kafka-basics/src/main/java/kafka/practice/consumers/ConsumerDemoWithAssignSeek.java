package kafka.practice.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static kafka.practice.constants.Constant.*;

public class ConsumerDemoWithAssignSeek {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoWithAssignSeek.class.getName());

    public static void main(String[] args) {
        new ConsumerDemoWithAssignSeek().startConsumerThread();
    }

    private void startConsumerThread() {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS_VALUE);

        // create kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // assign topic partition(s) to consumer
        TopicPartition topicPartition = new TopicPartition(TOPIC_VALUE, 0);
        consumer.assign(Collections.singleton(topicPartition));

        // seek
        long startingOffset = 2L;
        consumer.seek(topicPartition, startingOffset);

        int totalNumberOfRecordsToRead = 5;
        int count = 1;
        boolean isValid = true;

        // poll records
        while (isValid) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord consumerRecord : consumerRecords) {
                System.out.println("Key = " + consumerRecord.key());
                System.out.println("Value = " + consumerRecord.value());
                System.out.println("Topic = " + consumerRecord.topic());
                System.out.println("Partition = " + consumerRecord.partition());
                System.out.println("Offset = " + consumerRecord.offset());
                System.out.println("Timestamp = " + consumerRecord.timestamp());

                System.out.println();

                count += 1;

                if (count > totalNumberOfRecordsToRead) {
                    isValid = false;
                    break;
                }
            }
        }

        LOG.info(totalNumberOfRecordsToRead + " records have been read...");
        LOG.info("Exiting Application...");
    }
}
