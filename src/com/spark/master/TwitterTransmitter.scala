package com.spark.master

import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.JsonDSL._
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, Days}
import StreamingLogger._
import org.apache.spark._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.SQLContext

/**
 *
 * Sends relevant key-value pairs from the Tweets and pass them to ElasticSearch
 * To run this project, you need to have elasticsearch installed and running
 * Need sbt, OAuth credentials in config file
 * From command line
 *
 * $ cd ScreamingTwitter
 * $ sbt run
 * or
 * $ sbt 'run filters'
 *
 */
object TwitterTransmitter {
  def main(args: Array[String]) {

    // Define Logging Level
   setStreamingLogLevels()

    // Read configurations from a config file
   // val conf = ConfigFactory.load()


    // Accept Twitter Stream filters from arguments passed while running the job
    val filters = if (args.length == 0) List() else args.toList
    // create filter Strings
    //val filters : Array[String] = Array("Dallas"," Police Shooting")

    // Print the filters being used
    println("Filters being used: " + filters.mkString(","))

    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "kitWON6h8pqfC7z2Kai03U72y")
    System.setProperty("twitter4j.oauth.consumerSecret", "FhdO5UAdTFrPyKvHkGl8wLqxVmhrfFhojljNWegf9QKmmQ8p0l")
    System.setProperty("twitter4j.oauth.accessToken", "338883393-xxcZ1Jy3h6vm3gPCTWXNl5O095cqJq4OCI4t6K2B")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "gkQrToUEGfv3QmCypqGeYvXFfDEzYI89CNqIJGShwklpR")

    // Initialize a SparkConf with all available cores
    val sparkConf = new SparkConf().setAppName("PopularTag").setMaster("local[*]")
    
    // Create a StreamingContext with a batch interval of 10 seconds.
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    // Create a DStream the gets streaming data from Twitter with the filters provided
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Process each tweet in a batch
    val tweetMap = stream.map(status => {

      // Defined a DateFormat to convert date Time provided by Twitter to a format understandable by Elasticsearch

    val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")

    def checkObj (x : Any) : String  = {
        x match {
          case i:String => i
          case _ => ""
        }
      }


      // Creating a JSON using json4s.JSONDSL with fields from the tweet and calculated fields
      val tweetMap =
        ("UserID" -> status.getUser.getId) ~
          ("UserDescription" -> status.getUser.getDescription) ~
          ("UserScreenName" -> status.getUser.getScreenName) ~
          ("UserFriendsCount" -> status.getUser.getFriendsCount) ~
          ("UserFavouritesCount" -> status.getUser.getFavouritesCount) ~
          ("UserFollowersCount" -> status.getUser.getFollowersCount) ~
          // Ration is calculated as the number of followers divided by number of people followed
          ("UserFollowersRatio" -> status.getUser.getFollowersCount.toFloat / status.getUser.getFriendsCount.toFloat) ~
          //("Geo_Latitude" -> checkObj(status.getGeoLocation.getLatitude)) ~
          //("Geo_Longitude" -> checkObj(status.getGeoLocation.getLongitude)) ~
          ("UserLang" -> status.getUser.getLang) ~
          ("UserLocation" -> status.getUser.getLocation) ~
          ("UserVerification" -> status.getUser.isVerified) ~
          ("UserName" -> status.getUser.getName) ~
          ("UserStatusCount" -> status.getUser.getStatusesCount) ~
          // User Created DateTime is first converted to epoch miliseconds and then converted to the DateFormat defined above
          ("UserCreated" -> formatter.format(status.getUser.getCreatedAt.getTime)) ~
          ("Text" -> status.getText) ~
          ("TextLength" -> status.getText.length) ~
          //Tokenized the tweet message and then filtered only words starting with #
          ("HashTags" -> status.getText.split(" ").filter(_.startsWith("#")).mkString(" ")) ~
          ("StatusCreatedAt" -> formatter.format(status.getCreatedAt.getTime)) // ~
          //("PlaceName" -> status.getPlace.getName.toString()) ~
          //("PlaceCountry" -> status.getPlace.getCountry.toString())

      // This function takes Map of tweet data and returns true if the message is not a spam
      def spamDetector(tweet: Map[String, Any]): Boolean = {
        {
          // Remove recently created users = Remove Twitter users who's profile was created less than a day ago
          Days.daysBetween(new DateTime(formatter.parse(tweet.get("UserCreated").mkString).getTime),
            DateTime.now).getDays > 1
        } & {
          // Users That Create Little Content =  Remove users who have only ever created less than 50 tweets
          tweet.get("UserStatusCount").mkString.toInt > 50
        } & {
          // Remove Users With Few Followers
          tweet.get("UserFollowersRatio").mkString.toFloat > 0.01
        } & {
          // Remove Users With Short Descriptions
          tweet.get("UserDescription").mkString.length > 20
        } & {
          // Remove messages with a Large Numbers Of HashTags
          tweet.get("Text").mkString.split(" ").filter(_.startsWith("#")).length < 5
        } & {
          // Remove Messages with Short Content Length
          tweet.get("TextLength").mkString.toInt > 20
        } & {
          // Remove Messages Requesting Retweets & Follows
          val filters = List("rt and follow", "rt & follow", "rt+follow", "follow and rt", "follow & rt", "follow+rt")
          !filters.exists(tweet.get("Text").mkString.toLowerCase.contains)
        }
      }
      // If the tweet passed through all the tests in SpamDetector Spam indicator = FALSE else TRUE
      spamDetector(tweetMap.values) match {
        case true => tweetMap.values.+("Spam" -> false)
        case _ => tweetMap.values.+("Spam" -> true)
      }

    })

    //tweetMap.print
    tweetMap.map(s => List("Tweet Extracted")).print

        // Keep count of how many Tweets we've received so we can stop automatically
    // (and not fill up your disk!)
    var totalTweets:Long = 0
        
    tweetMap.foreachRDD((rdd, time) => {
      // Don't bother with empty batches
      if (rdd.count() > 0) {
        // Combine each partition's results into a single RDD:
        val repartitionedRDD = rdd.repartition(1).cache()
        // And print out a directory with the results.
        repartitionedRDD.saveAsTextFile("Twitter_" + time.milliseconds.toString)
        // Stop once we've collected 1000 tweets.
        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0)
        }
      }
    })
    
    val sc = new SparkContext() // An existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create the DataFrame
    val df = sqlContext.read.json("/Users/theophilus/Desktop/TwitterStream/StreamingTwitterMaster/src/com/spark/master/tweets.json")

   // Show the content of the DataFrame
    df.show()

    ssc.start  // Start the computation
    ssc.awaitTermination  // Wait for the computation to terminate
    
  }
}
