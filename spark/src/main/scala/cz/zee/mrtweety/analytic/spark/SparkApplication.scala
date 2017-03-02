package cz.zee.mrtweety.analytic.spark

import java.io.{File, IOException}
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.io.FileUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Durations, Minutes, StreamingContext}
import org.json.{JSONArray, JSONObject}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Spark main application class.
  *
  * @author Jakub Horak
  */
object SparkApplication {

  private final val LOG = LoggerFactory.getLogger(SparkApplication.getClass)

  private final val WORD_BLACKLIST = Set("europe", "europa", "eu", "euro")

  var resultFile: File = _

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MrTweety Analytic").setMaster("local[*]")

    // setup processing with batch interval
    val streamingContext = new StreamingContext(conf, Durations.seconds(10))

    // initialize the result file
    val resultFilename = System.getenv("RESULT_FILENAME")
    resultFile = new File(if (resultFilename != null) resultFilename else "analytic.json")
    LOG.info("Initialized result file at {}", resultFilename)

    // Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "tweet",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("tweet")

    // get the Kafka stream
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(record => record.value)

    // parse the hashtags from JSON
    val hashtags: DStream[String] = lines.flatMap(line => {
      val rootObject = new JSONObject(line)
      val entitiesObject = rootObject.optJSONObject("entities")
      if (entitiesObject != null) {
        val tweetHashtagsArray = entitiesObject.getJSONArray("hashtags")
        var list = ListBuffer[String]()
        for (i <- 0 to tweetHashtagsArray.length()) {
          val hashtagObject = tweetHashtagsArray.getJSONObject(i)
          val hashtag = hashtagObject.getString("text")

          // remove blacklisted words while ignoring case
          if (!WORD_BLACKLIST.contains(hashtag)) {
            list += hashtag
          }
        }
        list
      } else {
        List[String]()
      }
    })

    val hashtagCounts = hashtags.map(s => (s, 1))
      .reduceByKeyAndWindow((i1, i2) => i1 + i2, Minutes.apply(15))
      .map(s => (s._2, s._1))
      .transform(v1 => v1.sortByKey(ascending = false))

    hashtagCounts.foreachRDD(pair => {
      val topHashtags: Array[(Int, String)] = pair.take(5)
      save(topHashtags)
    })

    streamingContext.start()
    try {
      streamingContext.awaitTermination()
    } catch {
      case _: InterruptedException => LOG.info("Interrupted while awaiting termination.")
    }
  }

  /**
    * Saves a list of top hash tags as a JSON into a resultFile.
    * @param topHashtags list of tuples where key is count and value is the hashtag
    */
  private def save(topHashtags: Array[(Int, String)]) {

    val rootObject = new JSONObject()
    val items = new JSONArray()
    rootObject.put("items", items)

    for (pair <- topHashtags) {
      val stat = new JSONObject()
      stat.put("hashtag", pair._2)
      stat.put("count", pair._1)
      items.put(stat)
    }

    rootObject.put("datetime", OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))

    try {
      FileUtils.write(resultFile, rootObject.toString())
    } catch {
      case e: IOException =>
        LOG.error("Could not write result file " + resultFile, e)
    }
  }
}
