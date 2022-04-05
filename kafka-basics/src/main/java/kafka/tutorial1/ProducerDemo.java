package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) throws IOException, InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        int seqNo = 1;

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/Users/yogwalia/Documents/bash-scripts/k1.txt"))) {
            while (seqNo <= 2000) {
                String formattedString = "";
                System.out.println(formattedString);
                //bufferedWriter.write(formattedString);
                //bufferedWriter.newLine();

                // create a producer record
                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>("TOPIC-1", formattedString);

                // send data - asynchronous
                producer.send(record);

                // flush data
                producer.flush();

                ++seqNo;
                Thread.sleep(100);
            }
        }
        // flush and close producer
        producer.close();
    }
}
