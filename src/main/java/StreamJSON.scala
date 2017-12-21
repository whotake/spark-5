import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamJSON {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val outputDirectory = "data"
    val conf = new SparkConf()

    conf.setAppName("spark-sreaming")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(30))

    // Configure your Twitter credentials

    val apiKey = "g4T4VLIbjwQfxtHLm2TlNUZbE"
    val apiSecret = "vymzHV4hiCm2R4SHxUobC2Pe2rGdQsMK7xo8YLPsEgHMkTqMVy"
    val accessToken = "869172753307504641-kIa5GAnWu76bQM1Ls59M0hGcl0wAubQ"
    val accessTokenSecret = "DbPUGEqbahlFPiQ5D3ub9tBIirnryYxGbRtqQqc3ILSj0"

    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

    //in order to avoid IO.Exception
    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\");
    // Create Twitter Stream in JSON

    val tweets = TwitterUtils
      .createStream(ssc, None)
      .map(new Gson().toJson(_))
    val numTweetsCollect = 1000L
    var numTweetsCollected = 0L
    //Save tweets in file

    tweets.foreachRDD((rdd, time) => {
      val count = rdd.count()

      if (count > 0) {
        val outputRDD = rdd.coalesce(1)
        outputRDD.saveAsTextFile(outputDirectory)
        numTweetsCollected += count

        if (numTweetsCollected > numTweetsCollect) {
          System.exit(0)
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}