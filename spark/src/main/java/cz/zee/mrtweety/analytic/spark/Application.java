package cz.zee.mrtweety.analytic.spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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

        Authorization twitterAuth = new OAuthAuthorization(getOauthConfiguration());

        JavaReceiverInputDStream<Status> statuses = TwitterUtils.createStream(streamingContext, twitterAuth, new String[]{"europe", "eu"});

        JavaDStream<String> tweets = statuses.map(Status::getText);

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

    Configuration getOauthConfiguration() {
        Properties twitterProperties = load("/twitter.properties");

        Configuration twitterAuthConf = new ConfigurationBuilder()
                .setOAuthConsumerKey(twitterProperties.getProperty("consumerKey"))
                .setOAuthConsumerSecret(twitterProperties.getProperty("consumerSecret"))
                .setOAuthAccessToken(twitterProperties.getProperty("accessToken"))
                .setOAuthAccessTokenSecret(twitterProperties.getProperty("accessTokenSecret"))
                .build();

        return twitterAuthConf;
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
