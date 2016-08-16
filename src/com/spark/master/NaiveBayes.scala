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

import scala.collection.mutable
import scala.io.Source

import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SQLContext
import org.apache.log4j.lf5.LogLevel



import org.joda.time.{DateTime,Days}
import org.apache.spark.api.java.StorageLevels


/** Simple application to listen to a stream of Tweets and print them out sentiments */
object NaiveBayes {
  
  // Read corpus of words from text files
  val posWords = Source.fromFile("/Users/theophilus/Desktop/TwitterStream/TwitterStream/src/com/spark/streaming/pos-words.txt").getLines()
  val negWords = Source.fromFile("/Users/theophilus/Desktop/TwitterStream/TwitterStream/src/com/spark/streaming/neg-words.txt").getLines()
  val stopWords = Source.fromFile("/Users/theophilus/Desktop/TwitterStream/TwitterStream/src/com/spark/streaming/stop-words.txt").getLines()
  
  val posWordsArr = mutable.MutableList("")
  val negWordsArr = mutable.MutableList("")
  
  for(posword <-posWords){
    posWordsArr +=(posword)
  }
  for(negWord <-negWords){
    negWordsArr +=(negWord)
  }
  
  // custom function to find whether the tweet is 
  // positive or negative or neutral
  def findTweetSentiment(tweet:String):String = {
    var count = 0
    for(w <- tweet.split(" ")){
      //positive words
      for(positiveW <- posWordsArr){
        if(w != " " && positiveW.toString.toLowerCase()==w){
          count = count+1
        }
      }
      //negative words
      for(negativeW <- negWordsArr){
        if(w != " " && negativeW.toString.toLowerCase()==w){
          count = count-1
        }
      }
      
    }
    if(count > 0){
      // 1 - positive
      return "1"
    }
    else if(count < 0){
      // 0 - negative
      return "0"
    }
    else { 
     // 2 - Neutral
        return "2" 
     }
  }
  
  //only words function
   def onlyWords(text: String) : String = {
       text.split(" ").filter(_.matches("^[a-zA-Z0-9 ]+$")).fold("")((a,b) => a + " " + b).trim
   }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    //logger
    setStreamingLogLevels()

    // Accept Twitter Stream filters from arguments passed while running the job
     // val filters = if (args.length == 0) List() else args.toList
    // create filter Strings
     val filters : Array[String] = Array("Dallas Shooter","Police","Dallas Killing","Dallas Shootings","Dallas Police")
    
    // Print the filters being used
    // println("Filters being used: " + filters.mkString(","))
    
    //configuration
	  val conf = new SparkConf().setAppName("Twitter Sentiment Analyzer").setMaster("local[*]")
	                            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
	  // spark context
	  val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext(sc, Seconds(1))
    
  
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "kitWON6h8pqfC7z2Kai03U72y")
    System.setProperty("twitter4j.oauth.consumerSecret", "FhdO5UAdTFrPyKvHkGl8wLqxVmhrfFhojljNWegf9QKmmQ8p0l")
    System.setProperty("twitter4j.oauth.accessToken", "338883393-xxcZ1Jy3h6vm3gPCTWXNl5O095cqJq4OCI4t6K2B")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "gkQrToUEGfv3QmCypqGeYvXFfDEzYI89CNqIJGShwklpR")

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc,None,filters,StorageLevels.MEMORY_AND_DISK)
                             .filter { x => x.getLang == "en" }
    
   // get sentiments and tweet text
    val data = tweets.map { status =>
        
      // RETURN sentiments
        val sentiment = findTweetSentiment(status.getText().toLowerCase())
        
        // combine text with sentiments
        (onlyWords(status.getText), sentiment.toDouble)
     }
   
   // print top 10 
   data.print(10)
 
   val sqlContext = new SQLContext(sc)
   import sqlContext.implicits._
    
   import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, IDF, Tokenizer}
   import org.apache.spark.ml.PipelineStage
   import org.apache.spark.sql.Row
   import org.apache.spark.ml.classification.LogisticRegression
   import org.apache.spark.ml.{Pipeline, PipelineModel}
   import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
   import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
   import org.apache.spark.mllib.linalg.Vector
   
  
   data.foreachRDD { rdd =>
     
    // convert RDD into DataFrame
    val df = rdd.toDF("text","label")
    
    // register as tempTable
    df.registerTempTable("STable")
    
    // run some queries
    sqlContext.sql("SELECT text,label FROM STable").show()
    
    // Split data into training (60%) and test (40%).
    val splits = df.randomSplit(Array(0.6, 0.4), seed = 12345L)
    
    val training = splits(0).cache()
    val testing = splits(1)
    
    
    //******************************************************************************************//
    //  Naive Bayes Approach                                                                    //
    //******************************************************************************************//
    import org.apache.spark.ml.classification.NaiveBayes
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
      
    println("Total Document Count = " + df.count())
    println("Training Count = " + training.count() + ", " + training.count*100/(df.count()).toDouble + "%")
    println("Test Count = " + testing.count() + ", " + testing.count*100/(df.count().toDouble) + "%")
            
    // Configure an ML pipeline, which consists of five stages: tokenizer, remover, hashingTF,IDF and lr.
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered").setCaseSensitive(false)
    val hashingTF = new HashingTF().setNumFeatures(20000).setInputCol("filtered").setOutputCol("rawFeatures")
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(0)
    val nb = new NaiveBayes() 
    
    val pipeline = new Pipeline().setStages(Array(tokenizer, remover, hashingTF, idf, nb))
    
    // Fit the pipeline to training documents.
    val model = pipeline.fit(training)

    // Select example rows to display.
    val predictions = model.transform(testing)
    //predictions.show()
    
    predictions.select("text", "probability", "label","prediction").show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
                       .setLabelCol("label")
                       .setPredictionCol("prediction")
                       
    // evaluate for accuracy
    val accuracy = evaluator.evaluate(predictions)
    println("Accuracy: " + accuracy)
    println(evaluator.isLargerBetter)
    
    
    // Tune hyperparameters
    val paramGrid = new ParamGridBuilder().addGrid(hashingTF.numFeatures, Array(1000, 10000, 100000))
                    .addGrid(idf.minDocFreq, Array(0,10, 100))
                    .build()
                    
    
    // Cross validation
    val cv = new CrossValidator().setEstimator(pipeline)
                 .setEvaluator(evaluator)
                 .setEstimatorParamMaps(paramGrid).setNumFolds(2)
                 
   // cross-evaluate
    val cvModel = cv.fit(training)
    println("Area under the ROC curve for best fitted model = " + evaluator.evaluate(cvModel.transform(testing)))
    
    println("Area under the ROC curve for non-tuned model = " + evaluator.evaluate(predictions))
    println("Area under the ROC curve for best fitted model = " + evaluator.evaluate(cvModel.transform(testing)))
    println("Improvement = " + "%.2f".format((evaluator.evaluate(cvModel.transform(testing)) - evaluator.evaluate(predictions)) *100 / evaluator.evaluate(predictions)) + "%")
    
     
   }
    // Keep count of how many Tweets we've received so we can stop automatically
    // (and not fill up your disk!)
   /*var totalTweets:Long = 0
        
    data.foreachRDD((rdd, time) => {
      // Don't bother with empty batches
      if (rdd.count() > 0) {
        // Combine each partition's results into a single RDD:
        val repartitionedRDD = rdd.repartition(1).cache()
        // And print out a directory with the results.
        //repartitionedRDD.saveAsTextFile("PrintTweets_" + time.milliseconds.toString)
        
        // Stop once we've collected 1000 tweets.
        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0)
        }
      }
    })*/
    
    // SET CheckPoint
    ssc.checkpoint("/Users/theophilus/Documents/checkpoint")
    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }  
}