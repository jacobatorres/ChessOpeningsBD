package chesscode

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark._

import scala.io.Source

object ChessOpenings {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // parse the data to look the the table
    // make a SQL from it

    // i think sc.textFile automatically does the partitioning, and spark handles the number of partitions
    // if true, can I make a function per partition so that it's faster?

    // as per these references:
    // https://stackoverflow.com/questions/40892080/how-to-use-mappartitions-in-spark-scala
    // https://spark.apache.org/docs/2.1.0/programming-guide.html (ctrl + f textfile)
    // i should be able to do sc.textfile().mappartitions
    // the idea is that you split the big text file into partitions,
    // distribute these partitions into the workers, then do the function per partition
    // much faster than just having one worker grind everything

    val sc = new SparkContext ("local[*]", "Chess")
    val ChessRDD =  sc.textFile("data/TestData901lines.txt")

    val testprint = ChessRDD.mapPartitions(idx => Array(idx.size).iterator).collect

    testprint.foreach(println)

//
//    val spark = SparkSession
//      .builder
//      .appName("Test")
//      .master("local[*]")
//      .getOrCreate()
//
//
//    spark.stop()
//
//
//    println("Trying out personal token")



  }

}