package kafka.consumer;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Manikanta Tummalapenta on 30 Dec 2019
 */
public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) throws IOException {
        new Consumer().run();
    }

    private void run() throws IOException {
        // Create Consumer properties
        Properties properties = getConsumerProperties();

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Listen to messages
        consumer.subscribe(Arrays.asList("first_topic"));

        // Get ES Client
        RestHighLevelClient client = ElasticSearchClientHelper.getHighLevelClient();

        BulkRequest bulkRequest = new BulkRequest();

        // Poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordsCount = records.count();
            logger.info("Received " + recordsCount + " records");

            if (recordsCount > 0) {
                for (ConsumerRecord<String, String> record : records) {
                    // Generate your own id

                    try {
                        // Way:1 Kafka Generic Id
                        String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                        // Way:2 Twitter feed specific Id - source of each tweet has id
                        id = extractIdFromTweet(record.value());

                        insertMessageToES(client, record.value(), id, bulkRequest);
                    } catch (Exception ex) {
                        logger.warn("Ignoring exception...");
                    }
                }

                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                // Committing the offsets
                logger.info("Committing the offsets...");
                consumer.commitSync();
                logger.info("Offsets has been committed...");

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

//        client.close();
    }

    private static Properties getConsumerProperties() {
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        String groupId = "es_consumer_group";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Offset manual commit properties
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        return properties;
    }

    private static void insertMessageToES(RestHighLevelClient client, String message, String id, BulkRequest bulkRequest) throws IOException {

        if (null == message) {
            return;
        }

        IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                .source(message, XContentType.JSON);

        bulkRequest.add(indexRequest);
    }

    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String jsonMessage) {

        return jsonParser.parse(jsonMessage)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
