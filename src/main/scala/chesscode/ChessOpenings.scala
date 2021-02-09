package chesscode

import org.apache.spark.sql._
import org.apache.log4j._


object ChessOpenings {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Test")
      .master("local[*]")
      .getOrCreate()


    spark.stop()


    println("Yeyy")



  }

}