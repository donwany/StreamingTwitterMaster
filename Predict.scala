package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.StreamingKMeans

/**
 * Pulls live tweets and filters them for tweets in the chosen cluster.
 */
object Predict {
  def main(args: Array[String]) {
    
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "kitWON6h8pqfC7z2Kai03U72y")
    System.setProperty("twitter4j.oauth.consumerSecret", "FhdO5UAdTFrPyKvHkGl8wLqxVmhrfFhojljNWegf9QKmmQ8p0l")
    System.setProperty("twitter4j.oauth.accessToken", "338883393-xxcZ1Jy3h6vm3gPCTWXNl5O095cqJq4OCI4t6K2B")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "gkQrToUEGfv3QmCypqGeYvXFfDEzYI89CNqIJGShwklpR")

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName("PredictTweetModel").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    println("Initializing Twitter stream...")
    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(_.getText)

    //println("Initalizaing the the KMeans model...")
    //val model = new KMeansModel(ssc.sparkContext.objectFile[Vector](modelFile.toString).collect())

    //val filteredTweets = statuses
    //  .filter(t => model.predict(Utils.featurize(t)) == clusterNumber)
    //filteredTweets.print()
    
    statuses.print(10)

    // Start the streaming computation
    println("Initialization complete.")
    ssc.start()
    ssc.awaitTermination()
  }
}