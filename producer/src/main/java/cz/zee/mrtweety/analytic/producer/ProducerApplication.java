package cz.zee.mrtweety.analytic.producer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Produces Kafka messages from Twitter stream.
 * @author Jakub Horak
 */
public class ProducerApplication {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerApplication.class);
    private static final String KAFKA_TOPIC_TWEET = "tweet";

    public static void main(String[] args) {
        new ProducerApplication().start();
    }

    private void start() {
        Properties kafkaProducerProperties = load("/kafka-producer.properties");
        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProperties);

        BlockingQueue<String> queue = new LinkedBlockingDeque<>();

        StatusesFilterEndpoint streamingEndpoint = new StatusesFilterEndpoint();
        streamingEndpoint.trackTerms(Arrays.asList("europe", "europa", "#eu", "euro"));

        Map<String, String> envVars = System.getenv();
        Authentication authentication = new OAuth1(
                envVars.get("TWITTER_CONSUMER_KEY"), envVars.get("TWITTER_CONSUMER_SECRET"),
                envVars.get("TWITTER_ACCESS_TOKEN"), envVars.get("TWITTER_ACCESS_TOKEN_SECRET"));

        Client client = new ClientBuilder()
                .authentication(authentication)
                .hosts(Constants.STREAM_HOST)
                .endpoint(streamingEndpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();

        while (true) {
            ProducerRecord<String, String> message;
            try {
                message = new ProducerRecord<>(KAFKA_TOPIC_TWEET, queue.take());
            } catch (InterruptedException e) {
                LOG.error("Kafka interrupted", e);
                break;
            }
            producer.send(message);
        }
        producer.close();
        client.stop();
    }

    /**
     * Loads properties from the specified resource.
     * @param resourceName name of the resource
     * @return populated properties
     */
    private Properties load(String resourceName) {
        Properties properties = new Properties();
        try {
            properties.load(getClass().getResourceAsStream(resourceName));
        } catch (IOException e) {
            LOG.error("Could not load properties for resource {}", resourceName, e);
        }
        return properties;
    }

}

