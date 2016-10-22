package cz.zee.mrtweety.analytic.spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Spark main application class.
 * @author Jakub Horak
 */
public class Application implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    private static final Pattern SPACE = Pattern.compile(" ");

    private File resultFile;

    public static void main(String[] args) {
        new Application().start();
    }

    private void start() {

        SparkConf conf = new SparkConf().setAppName("MrTweety Analytic").setMaster("local[*]");

        // setup processing with batch interval
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        // initialize the result file
        String resultFilename = System.getenv("RESULT_FILENAME");
        resultFile = new File(resultFilename != null ? resultFilename : "analytic.json");
        LOG.info("Initialized result file at {}", resultFilename);

        // get the Kafka stream
        JavaPairReceiverInputDStream<String, String> messages =
                // Zookeeper location and Kafka topic
                KafkaUtils.createStream(streamingContext, "localhost:2181", "mrtweety-analytic", Collections.singletonMap("tweet", 1));

        JavaDStream<String> lines = messages.map(Tuple2::_2);

        // parse the tweet texts from JSON
        JavaDStream<String> tweets = lines.map(line -> {
            JSONObject jsonObject = new JSONObject(line);
            return jsonObject.optString("text");
        });
        JavaDStream<String> words = tweets.flatMap(x -> Arrays.asList(SPACE.split(x)))
                .filter(word -> word.startsWith("#"));

        JavaPairDStream<Integer, String> hashtagCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKeyAndWindow((i1, i2) -> i1 + i2, Minutes.apply(5))
                .mapToPair(s -> new Tuple2<>(s._2(), s._1()))
                .transformToPair(v1 -> v1.sortByKey(false));

        hashtagCounts.foreach(pair -> {
            List<Tuple2<Integer, String>> topHashtags = pair.take(5);
            save(topHashtags);
            return null;
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    /**
     * Saves a list of top hash tags as a JSON into a resultFile.
     * @param topHashtags list of tuples where key is count and value is the hashtag
     */
    private void save(List<Tuple2<Integer, String>> topHashtags) {

        JSONObject rootObject = new JSONObject();
        JSONArray items = new JSONArray();
        rootObject.put("items", items);

        topHashtags.forEach(tuple -> {
            JSONObject stat = new JSONObject();
            stat.put("hashtag", tuple._2());
            stat.put("count", tuple._1());

            items.put(stat);
        });

        try {
            FileUtils.write(resultFile, rootObject.toString());
        } catch (IOException e) {
            LOG.error("Could not write result file {}", resultFile, e);
        }
    }
}
