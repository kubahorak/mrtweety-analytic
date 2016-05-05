package cz.zee.mrtweety.analytic.consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONObject;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;

/**
 * Consumes Kafka messages.
 * @author Jakub Horak
 */
public class KafkaConsumer implements Serializable {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        new KafkaConsumer().start();
    }

    private void start() {
        SparkConf conf = new SparkConf().setAppName("MrTweety Analytic");

        // setup processing with batch interval
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        // get the Kafka stream
        JavaPairReceiverInputDStream<String, String> messages =
                // Zookeeper location and Kafka topic
                KafkaUtils.createStream(streamingContext, "localhost:2181", "mrtweety-analytic", Collections.singletonMap("tweet", 1));

        JavaDStream<String> lines = messages.map(Tuple2::_2);

        // parse the tweet texts from JSON
        JavaDStream<String> tweets = lines.map(line -> {
            JSONObject jsonObject = new JSONObject(line);
            return jsonObject.getString("text");
        });

        JavaDStream<String> words = tweets.flatMap(x -> Arrays.asList(SPACE.split(x)));

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
