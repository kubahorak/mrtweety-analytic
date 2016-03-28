package cz.zee.mrtweety.analytic.producer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Produces Kafka messages from Twitter stream.
 * @author Jakub Horak
 */
public class KafkaProducer {

    private static final String KAFKA_TOPIC_TWEET = "tweet";

    public static void main(String[] args) {
        new KafkaProducer().start();
    }

    public void start() {
        Properties kafkaProducerProperties = load("/kafka-producer.properties");
        ProducerConfig producerConfig = new ProducerConfig(kafkaProducerProperties);
        Producer<String, String> producer = new Producer<>(producerConfig);

        BlockingQueue<String> queue = new LinkedBlockingDeque<>();

        StatusesFilterEndpoint streamingEndpoint = new StatusesFilterEndpoint();
        streamingEndpoint.trackTerms(Arrays.asList("easter", "#monday"));

        Properties twitterProperties = load("/twitter.properties");
        Authentication authentication = new OAuth1(
                twitterProperties.getProperty("consumerKey"), twitterProperties.getProperty("consumerSecret"),
                twitterProperties.getProperty("accessToken"), twitterProperties.getProperty("accessTokenSecret"));

        Client client = new ClientBuilder()
                .authentication(authentication)
                .hosts(Constants.STREAM_HOST)
                .endpoint(streamingEndpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();

        for (int i = 0; i < 3; ++i) {
            KeyedMessage<String, String> message = null;
            try {
                message = new KeyedMessage<>(KAFKA_TOPIC_TWEET, queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(message);
        }
        producer.close();
        client.stop();
    }

    private Properties load(String resourceName) {
        Properties properties = new Properties();
        try {
            properties.load(getClass().getResourceAsStream(resourceName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
