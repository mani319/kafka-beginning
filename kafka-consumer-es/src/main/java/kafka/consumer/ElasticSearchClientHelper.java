package kafka.consumer;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * Created by Manikanta Tummalapenta on 30 Dec 2019
 */
public class ElasticSearchClientHelper {

    public static RestHighLevelClient getHighLevelClient() {

        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
    }
}
