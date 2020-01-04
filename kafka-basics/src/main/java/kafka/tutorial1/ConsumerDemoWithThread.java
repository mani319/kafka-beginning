package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Manikanta Tummalapenta on 29 Dec 2019
 */
public class ConsumerDemoWithThread {

    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    private ConsumerDemoWithThread() {
    }

    public class ConsumerRunnable implements Runnable {

        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        ConsumerRunnable(CountDownLatch latch,
                         Properties properties,
                         String topic) {
            this.latch = latch;

            // Create Consumer
            consumer = new KafkaConsumer<>(properties);

            // Listen to messages
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                // Poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Message Read \n" +
                                "Key: " + record.key() + "\n" +
                                "Value: " + record.value() + "\n" +
                                "Partition: " + record.partition() + "\n" +
                                "Offset: " + record.offset());
                    }
                }
            } catch (WakeupException ex) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code that we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // wakeup() method is a special method to interrupt consumer.poll()
            // It'll throw exception WakeUpException
            consumer.wakeup();
        }
    }

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        // Create Consumer properties
        Properties properties = getConsumerProperties();

        // Create latch and consumer runnable
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, properties, "twitter_tweets");

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    private static Properties getConsumerProperties() {
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        String groupId = "my_java_app_3";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }
}
