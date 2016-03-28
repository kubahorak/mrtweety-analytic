package cz.zee.mrtweety.analytic.consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
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

        // batch interval of 10 second
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // get the Kafka stream
        JavaPairReceiverInputDStream<String, String> messages =
                // Zookeeper location and Kafka topic
                KafkaUtils.createStream(jssc, "localhost:2181", "mrtweety-analytic", Collections.singletonMap("tweet", 1));

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        // TODO parse the tweet from JSON

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) throws Exception {
                return Arrays.asList(SPACE.split(x));
            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
