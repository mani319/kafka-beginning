package kafka.producer;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Manikanta Tummalapenta on 29 Dec 2019
 */
public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {

        // Create Message Queue and twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10000);
        Client client = TwitterClientHelper.getTwitterClient(msgQueue);

        // Create Producer Properties
        Properties properties = getProducerProperties();

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Establish client connection
        client.connect();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            logger.info("Stopping client...");
            client.stop();
            logger.info("Closing producer...");
            producer.close();
            logger.info("Application has exited");
        }
        ));

        // Poll messaged from client and publish to kafka topic
        pollAndPublishToKafka(client, msgQueue, producer);

        // Flush data and close producer
        producer.flush();
        producer.close();

        client.stop();
    }

    private static void pollAndPublishToKafka(Client client,
                                              BlockingQueue<String> msgQueue,
                                              KafkaProducer<String, String> producer) {
        int i = 0;
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            logger.info(msg);

            // Create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>("first_topic", msg);

            // Send Data - Asynchronous
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time when record is successfully sent or exception is thrown
                    if (e == null) {
                        // Message successfully sent
                        logger.info("Message sent \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing.." + e);
                    }
                }
            });

            i++;
        }
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // safe producer configurations
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput configurations
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        return properties;
    }
}
