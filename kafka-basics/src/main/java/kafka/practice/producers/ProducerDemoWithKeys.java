package kafka.practice.producers;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static kafka.practice.constants.Constant.*;

public class ProducerDemoWithKeys {


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_VALUE);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_VALUE);

        // create kafka producer
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record
        for (int i = 1; i <= 5; i++) {

            String key = "key_" + i;
            String value = "value_" + i;

            System.out.println("Key = " + key + " , Value = " + value + "\n");

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(TOPIC_VALUE, key, value);

            // send records - asynchronus call
            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    System.out.println("RecordMetadata : \n" +
                            "Topic : " + recordMetadata.topic() + "\n" +
                            "Partition : " + recordMetadata.partition() + "\n" +
                            "Offset : " + recordMetadata.offset() + "\n" +
                            "Timestamp : " + recordMetadata.timestamp());
                }
            }).get(); // block .send() - synchronus
        }

        // flush and close
        //producer.close();
    }
}
