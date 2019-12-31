package kafka.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.processor.HosebirdMessageProcessor;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;

/**
 * Created by Manikanta Tummalapenta on 29 Dec 2019
 */
public class TwitterClientHelper {

    private static final String CONSUMER_KEY = "GqPCuXvi6BIjXOzdbBPTLfhca";
    private static final String CONSUMER_SECERT = "6XOkY6gQT1uXSmde7NvrtlUHXU9hwBiRUOWtRQs8YhFC2eFKrf";
    private static final String TOKEN = "1211262585607057410-TXxsUGmniUss2Af3UNqEITI4FNTqyQ";
    private static final String TOKEN_SECRET = "FHOMqQ21rzGtzKkzSkwKFDh4bdYnqlA7JXmrjDs3vktkS";

    public static Client getTwitterClient(BlockingQueue<String> msgQueue) {
        OAuth1 auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECERT, TOKEN, TOKEN_SECRET);
        Hosts hosts = HttpHosts.STREAM_HOST;

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList("bitcoin", "usa", "politics", "football"));

        HosebirdMessageProcessor processor = new StringDelimitedProcessor(msgQueue);

        return buildClient(auth, hosts, endpoint, processor);
    }

    private static Client buildClient(OAuth1 auth,
                               Hosts hosts,
                               StreamingEndpoint endpoint,
                               HosebirdMessageProcessor processor) {

        return new ClientBuilder()
                .name("My-Java-Twitter-Client")
                .connectionTimeout(5000)
                .authentication(auth)
                .hosts(hosts)
                .endpoint(endpoint)
                .processor(processor)
                .build();
    }
}
