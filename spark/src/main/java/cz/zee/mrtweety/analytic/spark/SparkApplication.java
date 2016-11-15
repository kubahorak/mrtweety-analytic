package cz.zee.mrtweety.analytic.spark;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Spark main application class.
 * @author Jakub Horak
 */
public class SparkApplication implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SparkApplication.class);

    private static final Set<String> WORD_BLACKLIST;

    static {
        Set<String> set = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        set.add("europe");
        set.add("europa");
        set.add("eu");
        set.add("euro");
        WORD_BLACKLIST = Collections.unmodifiableSet(set);
    }

    private File resultFile;

    public static void main(String[] args) {
        new SparkApplication().start();
    }

    private void start() {

        SparkConf conf = new SparkConf().setAppName("MrTweety Analytic").setMaster("local[*]");

        // setup processing with batch interval
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        // initialize the result file
        String resultFilename = System.getenv("RESULT_FILENAME");
        resultFile = new File(resultFilename != null ? resultFilename : "analytic.json");
        LOG.info("Initialized result file at {}", resultFilename);

        // Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "tweet");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // get the Kafka stream
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(
                        Collections.singletonList("tweet"),
                        kafkaParams
                )
        );

        JavaDStream<String> lines = messages.map(ConsumerRecord::value);

        // parse the hashtags from JSON
        JavaDStream<String> hashtags = lines.flatMap(line -> {
            JSONObject rootObject = new JSONObject(line);
            JSONObject entitiesObject = rootObject.optJSONObject("entities");
            if (entitiesObject != null) {
                JSONArray tweetHashtagsArray = entitiesObject.getJSONArray("hashtags");
                Collection<String> list = new LinkedList<>();
                for (int i = 0; i < tweetHashtagsArray.length(); ++i) {
                    JSONObject hashtagObject = tweetHashtagsArray.getJSONObject(i);
                    String hashtag = hashtagObject.getString("text");

                    // remove blacklisted words while ignoring case
                    if (!WORD_BLACKLIST.contains(hashtag)) {
                        list.add(hashtag);
                    }
                }
                return list.iterator();
            } else {
                return Collections.emptyIterator();
            }
        });

        JavaPairDStream<Integer, String> hashtagCounts = hashtags.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKeyAndWindow((i1, i2) -> i1 + i2, Minutes.apply(5))
                .mapToPair(s -> new Tuple2<>(s._2(), s._1()))
                .transformToPair(v1 -> v1.sortByKey(false));

        hashtagCounts.foreachRDD(pair -> {
            List<Tuple2<Integer, String>> topHashtags = pair.take(5);
            save(topHashtags);
        });

        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            LOG.info("Interrupted while awaiting termination.");
        }
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
