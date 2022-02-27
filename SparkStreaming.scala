import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Correct usage: SparkStreaming consumerKey consumerSecret accessToken accessTokenSecret")
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("SparkStreaming")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    //create datastream
    val stream = TwitterUtils.createStream(ssc, None, filters)

    //filter to get English tweets
    val filteredTweets = stream.filter(tweet => tweet.getLang=="en").map(status => status.getText())

    filteredTweets.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { part =>
        val props = new Properties()
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("bootstrap.servers", "localhost:9092")
        val producer = new KafkaProducer[String, String](props)

        part.foreach { tweet =>
          producer.send(new ProducerRecord[String, String]("test", "sentiment", SentimentAnalyzer.mainSentiment(tweet.toString).toString ))
        }
        producer.flush()
        producer.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
