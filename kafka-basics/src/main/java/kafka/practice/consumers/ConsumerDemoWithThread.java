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

    private ConsumerThread consumerThread;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().startConsumerThread();
    }

    private void startConsumerThread() {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS_VALUE);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_VALUE);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_VALUE);

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
            LOG.info("Application exit has started...");
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
                        System.out.println("Key = " + consumerRecord.key() + "\n");
                        System.out.println("Value = " + consumerRecord.value() + "\n");
                        System.out.println("Topic = " + consumerRecord.topic() + "\n");
                        System.out.println("Partition = " + consumerRecord.partition() + "\n");
                        System.out.println("Offset = " + consumerRecord.offset() + "\n");
                        System.out.println("Timestamp = " + consumerRecord.timestamp() + "\n");
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
