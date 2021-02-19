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

    val t1 = System.nanoTime


    val sc = new SparkContext ("local[*]", "Chess")
    val ChessRDD =  sc.textFile("data/test5000lines.txt")

    val partitions = ChessRDD.mapPartitions(idx => Array(idx.size).iterator).collect
    partitions.foreach(println)

    // mappartition start
    val recordsRDD = ChessRDD.mapPartitions(idx => {

      // go to the nearest "Event"
      // idx.hasNext ensures that there's a next

      val GameRecordList = mutable.MutableList[GameRecord]()

      try {

        while(idx.hasNext){

          var temp = idx.next
          while (!temp.contains("[Event")){
            temp = idx.next
          }

          var timeControl : String = null;
          var opening : String = null;
          var eco : String = null;
          var winner : String = null;
          var siteofplay : String = null;
          var whiteElo : Int = 0;
          var blackElo : Int = 0;


          // at this point, the next 14 lines are the important data so we get them
          for (a <- 0 to 14){

            if(a == 1){
              // site of play
              siteofplay = temp.split(" ")(1).drop(1).dropRight(2)

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

            } else if (a == 7) {
              // white ELO
              whiteElo = temp.split(" ")(1).drop(1).dropRight(2).toInt

            } else if (a == 8){
              // black ELO
              blackElo = temp.split(" ")(1).drop(1).dropRight(2).toInt

            } else if (a == 11 ){
              // ECO (opening code)
              eco = temp.split(" ")(1).drop(1).dropRight(2)

            } else if (a == 12){
              // opening
              opening = temp.drop(10).dropRight(2)

            } else if (a == 13) {
              // timecontrol
              timeControl = temp.split(" ")(1).drop(1).dropRight(2)

            }
            // ...



            temp = idx.next

          }
          // once youre done creating and recording one game, append it to the GamerecordList

          println("done with one record")
          val newRecord = GameRecord(timeControl, opening, eco, winner, siteofplay, whiteElo, blackElo)
          GameRecordList += newRecord

        }

      } catch {
        case e: NoSuchElementException => println("End of stream error caught...")

        case _: Throwable => println("Got some other kind of weird exception")
      }

      // now, GameRecordList should be List(GameRecord(..,..,..,), GameRecord(..,..,..), ... )


      Seq(GameRecordList).iterator


    }).collect().flatten

    val spark = SparkSession
      .builder
      .appName("ChessOpeningsBD")
      .master("local[*]")
      .getOrCreate()


    // this part converts the RDD to a DataFrame
    import spark.implicits._
    val df = recordsRDD.toSeq.toDF()

    val finaldf = df.withColumn("averageElo", (df("whiteElo") + df("blackElo")) / 2)


    // finaldf.show(10)
    finaldf.createOrReplaceTempView("tbl_ChessRecords")

    var timecontrolinput = "600+0"
    var eloinput = 2000
    var winnerinput = 'W'


    // add in the averageElo of the two players
    // then plug in the timeConrol = 600+0 and the EloCode = 2100

    println(s"\n\n\nResults: \nTimeControl: $timecontrolinput, Elo: $eloinput")

    println("Play as: White ")

    spark.sql(s"select cr.opening, cr.winner, cr.timecontrol, count(*) " +
      s"from tbl_ChessRecords cr where cr.timeControl = '$timecontrolinput' " +
      s" and cr.averageElo between ($eloinput - 300) AND ($eloinput + 300) " +
      s" and cr.winner = 'W' " +
      s"group by cr.opening, cr.winner, cr.timecontrol order by 4 desc").show(10, false)

    println("Play as: Black ")

    spark.sql(s"select cr.opening, cr.winner, cr.timecontrol, count(*) " +
      s"from tbl_ChessRecords cr where cr.timeControl = '$timecontrolinput' " +
      s" and cr.averageElo between ($eloinput - 300) AND ($eloinput + 300) " +
      s" and cr.winner = 'B' " +
      s"group by cr.opening, cr.winner, cr.timecontrol order by 4 desc").show(10, false)




    sc.stop()
    spark.stop()

    /* your code */

    val duration = (System.nanoTime - t1) / 1e9d // 1e9d is 10^9. Nanosecond is too many! So dividing it by 10^9 makes it readable
    println(s"Duration of the run: $duration seconds")
  }

}