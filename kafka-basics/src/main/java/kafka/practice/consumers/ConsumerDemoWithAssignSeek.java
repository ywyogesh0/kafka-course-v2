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

    private ConsumerThread consumerThread;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        new ConsumerDemoWithAssignSeek().startConsumerThread();
    }

    private void startConsumerThread() {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS_VALUE);

        // start consumer thread
        consumerThread = new ConsumerThread(properties);
        new Thread(consumerThread).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook has called...");
            consumerThread.shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                LOG.error("Shutdown hook has interrupted...", e);
            }
            LOG.info("Application has exited...");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            LOG.error("Application has interrupted...", e);
        } finally {
            LOG.info("Application is exiting...");
        }
    }

    class ConsumerThread implements Runnable {

        private Consumer<String, String> consumer;

        ConsumerThread(Properties properties) {
            // create kafka consumer
            consumer = new KafkaConsumer<>(properties);

            // assign topic partition(s) to consumer
            TopicPartition topicPartition = new TopicPartition(TOPIC_VALUE, 1);
            consumer.assign(Collections.singleton(topicPartition));

            // seek
            consumer.seek(topicPartition, 5L);
        }

        @Override
        public void run() {

            // poll records
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord consumerRecord : consumerRecords) {
                        System.out.println("Key = " + consumerRecord.key());
                        System.out.println("Value = " + consumerRecord.value());
                        System.out.println("Topic = " + consumerRecord.topic());
                        System.out.println("Partition = " + consumerRecord.partition());
                        System.out.println("Offset = " + consumerRecord.offset());
                        System.out.println("Timestamp = " + consumerRecord.timestamp());

                        System.out.println();
                    }
                }
            } catch (WakeupException we) {
                LOG.error("Consumer has been interrupted...");
            } finally {
                consumer.close();
                countDownLatch.countDown();
            }

        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
