package chesscode

import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source

object ChessOpenings {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // parse the data to look the the table
    // make a SQL from it

    // i think sc.textFile automatically does the partitioning, and spark handles the number of partitions
    // if true, can I make a function per partition so that it's faster?

    
    for (line <- Source.fromFile("data/TestData901lines.txt").getLines){
      println(line)
    }


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