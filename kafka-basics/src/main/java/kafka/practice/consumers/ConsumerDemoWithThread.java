package kafka.practice.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static kafka.practice.constants.Constant.*;

public class ConsumerDemoWithThread {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

    private Runnable consumerThread;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().startConsumerThread();
    }

    private void startConsumerThread() {

        String groupIdValue = "g2";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIdValue);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_VALUE);

        // start consumer thread
        consumerThread = new ConsumerThread(properties);
        new Thread(consumerThread).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook has called...");
            ((ConsumerThread) consumerThread).shutdown();

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

            // subscribe consumer to topic(s)
            consumer.subscribe(Collections.singleton(TOPIC_VALUE));
        }

        @Override
        public void run() {

            // poll records
            try {
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
