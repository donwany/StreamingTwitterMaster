

package com.spark.streaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Config._
import twitter4j.Twitter

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    //logger
    setStreamingLogLevels()

    // create filter Strings
    val filters : Array[String] = Array("Dallas","Dallas Police","Dallas Shootings","Philando Castile","Minnesota Police Shootings")
    
    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "kitWON6h8pqfC7z2Kai03U72y")
    System.setProperty("twitter4j.oauth.consumerSecret", "FhdO5UAdTFrPyKvHkGl8wLqxVmhrfFhojljNWegf9QKmmQ8p0l")
    System.setProperty("twitter4j.oauth.accessToken", "338883393-xxcZ1Jy3h6vm3gPCTWXNl5O095cqJq4OCI4t6K2B")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "gkQrToUEGfv3QmCypqGeYvXFfDEzYI89CNqIJGShwklpR")

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc,None,filters)
    
    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())
    
    // Print out the first ten
    statuses.print(10)
    
    // Keep count of how many Tweets we've received so we can stop automatically
    // (and not fill up your disk!)
    var totalTweets:Long = 0
        
    statuses.foreachRDD((rdd, time) => {
      // Don't bother with empty batches
      if (rdd.count() > 0) {
        // Combine each partition's results into a single RDD:
        val repartitionedRDD = rdd.repartition(1).cache()
        // And print out a directory with the results.
        repartitionedRDD.saveAsTextFile("PrintTweets_" + time.milliseconds.toString)
        // Stop once we've collected 1000 tweets.
        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0)
        }
      }
    })
    
    // SET CheckPoint
    ssc.checkpoint("/Users/theophilus/Documents/checkpoint")
    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }  
}