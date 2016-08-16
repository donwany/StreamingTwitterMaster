package com.spark.streaming

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

import com.google.gson.Gson

import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.jackson.Serialization

 import org.json4s.JsonWriter
 import org.json4s.DefaultFormats
 import org.json.JSONWriter
 import org.json4s.ToJsonWritable
 import org.apache.hadoop.yarn.webapp.ToJSON
 import org.apache.spark.api.java.StorageLevels


/**
 *
 * Sends relevant key-value pairs from the Tweets
 
 *
 */
object TwitterTransmitter {
  
  //private var gson = new Gson()
  
  
	//only words function
	def onlyWords(text: String) : String = {
			text.split(" ").filter(_.matches("^[a-zA-Z0-9 ]+$")).fold("")((a,b) => a + " " + b).trim
	}

  
  def main(args: Array[String]) {

    // Define Logging Level
   setStreamingLogLevels()

    // Accept Twitter Stream filters from arguments passed while running the job
    //val filters = if (args.length == 0) List() else args.toList
    // create filter Strings
    val filters : Array[String] = Array("Dallas Shooter","Dallas Killing","Dallas Shootings","Dallas Police")

   // Print the filters being used
   // println("Filters being used: " + filters.mkString(","))

    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "kitWON6h8pqfC7z2Kai03U72y")
    System.setProperty("twitter4j.oauth.consumerSecret", "FhdO5UAdTFrPyKvHkGl8wLqxVmhrfFhojljNWegf9QKmmQ8p0l")
    System.setProperty("twitter4j.oauth.accessToken", "338883393-xxcZ1Jy3h6vm3gPCTWXNl5O095cqJq4OCI4t6K2B")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "gkQrToUEGfv3QmCypqGeYvXFfDEzYI89CNqIJGShwklpR")

    // Initialize a SparkConf with all available cores
    val sparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    
    // Create a StreamingContext with a batch interval of 1 seconds.
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    
   // Create a DStream the gets streaming data from Twitter with the filters provided
    val stream = TwitterUtils.createStream(ssc, None, filters,StorageLevels.MEMORY_AND_DISK)
    
    // Process each tweet in a batch
    val tweetMap = stream.map(status => {

      // Defined a DateFormat to convert date Time provided by Twitter to a format understandable

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
          // Ratio is calculated as the number of followers divided by number of people followed
          ("UserFollowersRatio" -> status.getUser.getFollowersCount.toFloat / status.getUser.getFriendsCount.toFloat) ~
          ("GeoLatitude" -> status.getGeoLocation.getLatitude.toDouble) ~
          ("GeoLongitude" -> status.getGeoLocation.getLongitude.toDouble) ~
          ("UserLang" -> status.getUser.getLang) ~
          ("UserLocation" -> status.getUser.getLocation) ~
          ("UserVerification" -> status.getUser.isVerified) ~
          ("UserName" -> status.getUser.getName) ~
          ("UserStatusCount" -> status.getUser.getStatusesCount) ~
          // User Created DateTime is first converted to epoch miliseconds and then converted to the DateFormat defined above
          ("UserCreated" -> formatter.format(status.getUser.getCreatedAt.getTime)) ~
          ("Text" -> status.getText.toString()) ~
          ("TextLength" -> status.getText.length) ~
          //("PlaceName" -> (status.getPlace.getName.toString()))~
          //("PlaceCountry" -> (status.getPlace.getCountry.toString()))~
          //Tokenized the tweet message and then filtered only words starting with #
          ("HashTags" -> status.getText.split(" ").filter(_.startsWith("#")).mkString(" ")) ~
          ("StatusCreatedAt" -> formatter.format(status.getCreatedAt.getTime)) // ~
          

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
      
     //print tweets to console
      println(compact(render(tweetMap)))
      
     
      
    })
 
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
        //println("Tweet count: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0)
        }
      }
    })
    
  
    ssc.start  // Start the computation
    ssc.awaitTermination  // Wait for the computation to terminate
    
  }
}
