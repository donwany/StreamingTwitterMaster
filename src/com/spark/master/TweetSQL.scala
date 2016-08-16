package com.spark.streaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SQLContext
//import Config._

import org.apache.log4j.Level
import java.util.regex.Pattern
import org.apache.log4j.Level

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging


/** Create a RDD of lines from a text file, and keep count of
 *  how often each word appears.
 */
object TweetSQL {
  
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger} 
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
  
  // Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface.
    
  def main(args: Array[String]) {

    //logger
    setupLogging()
    
    // Set up a SparkContext named WordCount that runs locally using
    // all available cores.
    val conf = new SparkConf().setMaster("local[*]").setAppName("TweetSQL")
    //initialized sparkcontext
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    // sc is an existing SparkContext.
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Create an RDD of Person objects and register it as a table.
    val tweets = sqlContext.read.json("/Users/theophilus/Desktop/TwitterStream/TwitterStream/src/com/spark/streaming/twitter.json")
    
    //print schema
    tweets.printSchema()
    
    //show table
    tweets.show()
    
    //register temp table
    tweets.registerTempTable("TweetTable")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    sqlContext.sql("SELECT Text FROM TweetTable WHERE UserLang='en' LIMIT 10").show()
  
    //get tweets lang, name and text
    sqlContext.sql("SELECT UserLang, UserName, Text FROM TweetTable WHERE UserLang='en' LIMIT 10").show()
                    
     //count languages with the highest tweets
    sqlContext.sql("SELECT UserLang, COUNT(*) AS Counts FROM TweetTable " +
                    "GROUP BY UserLang ORDER BY Counts DESC LIMIT 1000").show()
         
    //get highest location counts
    sqlContext.sql("SELECT UserLocation, COUNT(*) AS LoCount FROM TweetTable "+
                 "GROUP BY UserLocation ORDER BY LoCount DESC LIMIT 10").show()
                 
     // get UserFriends Count
     sqlContext.sql("SELECT UserName,UserFriendsCount,UserLang FROM TweetTable "+
                 "GROUP BY UserFriendsCount,UserName,UserLang ORDER BY UserFriendsCount DESC  LIMIT 10").show()
     
      // get all verified users UserVerification
      sqlContext.sql("SELECT DISTINCT(UserName),UserVerification FROM TweetTable ").show()
     
    sc.stop
    
   }  
}