import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object Twitter {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("Twitter_stream_GRIAT")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    // Configure your Twitter credentials
    val apiKey = "g4T4VLIbjwQfxtHLm2TlNUZbE"
    val apiSecret = "vymzHV4hiCm2R4SHxUobC2Pe2rGdQsMK7xo8YLPsEgHMkTqMVy"
    val accessToken = "869172753307504641-kIa5GAnWu76bQM1Ls59M0hGcl0wAubQ"
    val accessTokenSecret = "DbPUGEqbahlFPiQ5D3ub9tBIirnryYxGbRtqQqc3ILSj0"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Create Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.map(t => t.getText)
    val splitedTweets = tweets.map(s => s.split(" "))
    val filteredHashTags = splitedTweets.map(s => s.filter(s => s.startsWith("#")))

    filteredHashTags.map(a => a.mkString(" ")).print()

    ssc.start()
    ssc.awaitTermination()
  }
}