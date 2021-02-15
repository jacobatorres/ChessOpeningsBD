package chesscode

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark._

import scala.collection.mutable
import scala.io.Source
case class GameRecord(timeControl: String, opening:String, eco: String, winner: String, siteOfPlay: String,
                      whiteElo: Int, blackElo: Int)

object ChessOpenings {



  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val sc = new SparkContext ("local[*]", "Chess")
    val ChessRDD =  sc.textFile("data/TestData901lines.txt")

    val partitions = ChessRDD.mapPartitions(idx => Array(idx.size).iterator).collect
    partitions.foreach(println)

    // mappartition start


    val recordsRDD = ChessRDD.mapPartitions(idx => {

      // go to the nearest "Event"
      // idx.hasNext ensures that there's a next

      // TimeFormat, Opening, EloCode, Winner, SiteOfGame, WhiteElo, BlackElo
      // String      , String, String, Char,   String,     Int,      Int
      var seqPartition = ()

      val GameRecordList = mutable.MutableList[GameRecord]()

      try {

        while(idx.hasNext){

          var temp = idx.next
          while (!temp.contains("[Event")){
            temp = idx.next
          }

          var z = new Array[String](10)

          var timeControl : String = null;
          var opening : String = null;
          var eco : String = null;
          var winner : String = null;
          var siteofplay : String = null;
          var whiteElo : Int = 0;
          var blackElo : Int = 0;

          for (a <- 0 to 14){

            if(a == 1){
              // site of play
              siteofplay = temp.split(" ")(1).drop(1).dropRight(2)

              println(s"site of play: $siteofplay")
            } else if (a == 4){
              // result and or winner
              var tempvar = temp.split(" ")(1).drop(1).dropRight(2)

              if (tempvar == "1-0"){
                winner = "W"
              } else if (tempvar == "0-1"){
                winner = "B"
              } else {
                winner = "D"
              }
              println(s"winner: $winner")

            } else if (a == 7) {
              // white ELO
              whiteElo = temp.split(" ")(1).drop(1).dropRight(2).toInt
              println(s"whiteELO: $whiteElo")
            } else if (a == 8){
              // black ELO

              blackElo = temp.split(" ")(1).drop(1).dropRight(2).toInt
              println(s"blackElo: $blackElo")
            } else if (a == 11 ){
              // ECO (opening code)
              eco = temp.split(" ")(1).drop(1).dropRight(2)
              println(s"ECO: $eco")

            } else if (a == 12){
              // opening
              opening = temp.drop(10).dropRight(2)
              println(s"opening: $opening")

            } else if (a == 13) {
              // timecontrol
              timeControl = temp.split(" ")(1).drop(1).dropRight(2)
              println(s"timecontrol: $timeControl")

            }
            // ...



            temp = idx.next

          }
          // https://stackoverflow.com/questions/39397652/convert-scala-list-to-dataframe-or-dataset

          // once youre done creating and recording the game, append it to the main
          val newRecord = GameRecord(timeControl, opening, eco, winner, siteofplay, whiteElo, blackElo)
          GameRecordList += newRecord

        }

      } catch {
        case e: NoSuchElementException => println("End of stream error caught...")
      }


      Seq(GameRecordList).iterator


    }).collect().flatten

    val spark = SparkSession
      .builder
      .appName("SparkSQLtest")
      .master("local[*]")
      .getOrCreate()


    // make the dataset adhere to the Friends class
    // make a Friends schema with the dataset
    import spark.implicits._
    val df = recordsRDD.toSeq.toDF()
    df.createOrReplaceTempView("tbl_ChessRecords")

    df.show(10)

    val res = spark.sql("select * from tbl_ChessRecords cr where cr.timeControl ='60+0' ").collect().foreach(println)






    sc.stop()
    spark.stop()
  }

}