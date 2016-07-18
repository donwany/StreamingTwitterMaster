package com.spark.streaming

import org.apache.spark._
import Config._

/** Create a RDD of lines from a text file, and keep count of
 *  how often each word appears.
 */
object WordCount {
  
  def main(args: Array[String]) {
     //logger
     setupLogging()
     
      //only words function
      def onlyWords(text: String) : String = {
         text.split(" ").filter(_.matches("^[a-zA-Z0-9 ]+$")).fold("")((a,b) => a + " " + b).trim
      }
     // Set up a SparkContext named WordCount that runs locally using
      // all available cores.
      val conf = new SparkConf().setMaster("local[*]").setAppName("WordCounting")
      //initialized sparkcontext
      val sc = new SparkContext(conf)
      // Create a RDD of lines of text in our text file
      val input = sc.textFile("/Users/theophilus/Desktop/TwitterStream/TwitterStream/src/com/spark/streaming/words.txt",minPartitions=1)
         
      // Use flatMap to convert this into an rdd of each word in each line
      val words = input.flatMap(line => line.split(" "))
      // filters values greater than 0 only
      val wordlength = words.filter(_.length > 4)
    
      // Convert these words to lowercase
      val lowerCaseWords = wordlength.map(word => word.toLowerCase())
      // Count up the occurrence of each unique word
      val wordCounts = lowerCaseWords.map(word => (word,1))
      //combine wordcount by key
      val wordadd = wordCounts.reduceByKey((x,y) => x+y)
      //sorting by values
      val sortedcount = wordadd.sortBy(_._2, ascending=false)
      
      //print every element
      sortedcount.foreach(println)
      
      //take top 20
      //sortedcount.top(20).foreach{case(word,count)=>println(f"$word:$count")}
      
      //take first 10  
      //sortedcount.take(10).foreach{case(word,count) => println(f"$word%20s:$count%04d")}
      
      //write wordcount to file
      sortedcount.saveAsTextFile("word_count_output")
      
      sc.stop()
  
    }  
}