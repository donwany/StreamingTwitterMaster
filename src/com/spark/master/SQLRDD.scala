package com.spark.master

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SQLContext
//import Config._

/** Create a RDD of lines from a text file, and keep count of
 *  how often each word appears.
 */
object SQLRDD {
  
  // Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface.
  case class Person(name: String, age: Int)
    
  def main(args: Array[String]) {

     // Set up a SparkContext named WordCount that runs locally using
    // all available cores.
    val conf = new SparkConf().setMaster("local[*]").setAppName("PeopleDframe")
    //initialized sparkcontext
    val sc = new SparkContext(conf)
    
    // sc is an existing SparkContext.
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Create an RDD of Person objects and register it as a table.
    val people = sc.textFile("/Users/theophilus/Desktop/TwitterStream/StreamingTwitterMaster/src/com/spark/master/people.txt")
                 
    // Create data frame
    val peoplesdf = people.map(_.split(" ")).map(p => Person(name=p(0).toString, age=p(1).trim.toInt)).toDF()
    
    //print schema
    peoplesdf.printSchema()
    peoplesdf.show
    
    //register temp table
    peoplesdf.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 30 AND age <= 40")

    //another SQL query
    val nameCol = sqlContext.sql("SELECT name,age FROM people WHERE age = 31")
    nameCol.show()
    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index:
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    // or by field name:
    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
    // Map("name" -> "Justin", "age" -> 19)
    
    val df = sqlContext.read.format("json").
    load("/Users/theophilus/Desktop/TwitterStream/StreamingTwitterMaster/src/com/spark/master/pycon.json")
    
    //df.select("users").write.format("parquet").save("users.parquet")
    df.show()
   }  
}