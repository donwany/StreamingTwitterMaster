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

object dframe {
  
  def main(args: Array[String]) {

    // Set up a SparkContext named WordCount that runs locally using
    // all available cores.
    val conf = new SparkConf().setMaster("local[*]").setAppName("CreatingDataFrame")
    //initialized sparkcontext
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)

    // Create the DataFrame
    val df = sqlContext.read.json("/Users/theophilus/Desktop/TwitterStream/StreamingTwitterMaster/src/com/spark/master/tweets.json")

   // Show the content of the DataFrame
    df.show()
    //print the schema
    df.printSchema()

    sc.stop()  // Start the computation
    //sc.awaitTermination  // Wait for the computation to terminate
    
  }
}
