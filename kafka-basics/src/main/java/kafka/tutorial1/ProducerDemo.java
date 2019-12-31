package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by Manikanta Tummalapenta on 29 Dec 2019
 */
public class ProducerDemo {

    public static void main(String[] args) {

        // Create Producer Properties
        Properties properties = getProducerProperties();

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("first_topic", "Hello!");

        // Send Data - Asynchronous
        producer.send(producerRecord);

        // Flush data and close producer
        producer.flush();
        producer.close();
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
