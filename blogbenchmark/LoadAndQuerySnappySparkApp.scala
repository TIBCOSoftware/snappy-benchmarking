import java.io.{File, FileOutputStream, PrintStream, PrintWriter}

import scala.util._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SnappyContext, SnappySession, SparkSession}

object LoadAndQuerySnappySparkApp {
  def main(args: Array[String]): Unit = {

    val connectionURL = args(args.length - 1)

    val sparkSession: SparkSession = SparkSession
        .builder.appName("LoadAndQueryPerfSparkApp for Snappy").config("snappydata.connection", connectionURL)
        .getOrCreate

    val snc = new SnappySession(sparkSession.sparkContext)
    val pw = new PrintWriter(new FileOutputStream(new File("LoadAndQueryPerfSnappySparkApp_Azure" +
        ".out"),
      true))

    Try {
      val airlineDataFrame = CommonObject.getAirlineDataFrame(snc.sqlContext)
      val airlineRefDataFrame = CommonObject.getAirlineRefDataFrame(snc.sqlContext)

      snc.dropTable("airline", ifExists = true)
      snc.dropTable("airlineref", ifExists = true)

      val p1 = Map("PERSISTENCE" -> "NONE")
      snc.createTable("airline", "column", airlineDataFrame.schema, p1)
      var start = System.currentTimeMillis
      airlineDataFrame.write.insertInto("airline")
      var end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airline = " +
          (end - start) + " ms")

      snc.sql(
        s"""
           |CREATE TABLE airlineref
           |(code VARCHAR(20),description VARCHAR(50))
           |USING row OPTIONS()
      """.stripMargin)

      start = System.currentTimeMillis
      airlineRefDataFrame.write.insertInto("airlineref")
      end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airlineref = " +
          (end - start) + " ms")


      val Q1 = "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier from airline group by UniqueCarrier order by arrivalDelay"
      val Q2 = "select  count(*) flightRecCount, DESCRIPTION AirlineName, UniqueCarrier carrierCode ,Year from airline , airlineref where airline.UniqueCarrier = airlineref.CODE group by UniqueCarrier,DESCRIPTION, Year order by flightRecCount desc limit 10"
      val Q3 = "select AVG(ArrDelay) arrivalDelay, DESCRIPTION AirlineName, UniqueCarrier carrier from airline, airlineref where airline.UniqueCarrier = airlineref.CODE group by UniqueCarrier, DESCRIPTION order by arrivalDelay"
      val Q4 = "select AVG(ArrDelay) ArrivalDelay, Year from airline group by Year order by Year"
      val Q5 = "SELECT sum(WeatherDelay) totalWeatherDelay, airlineref.DESCRIPTION FROM airline, airlineref WHERE airline.UniqueCarrier = airlineref.CODE AND  Origin like '%SFO%' AND WeatherDelay > 0 GROUP BY DESCRIPTION limit 20"
      val Q6 = "select ArrDelay arrivalDelay from airline where UniqueCarrier = 'US' AND FLIGHTNUM = 401 AND MONTH = 12 AND YEAR = 2014 AND DAYOFMONTH = 1"
      val Q7 = "select WeatherDelay from airline where UniqueCarrier = 'MQ' AND FLIGHTNUM = 2718 AND DAYOFMONTH = 21 AND MONTH = 2 AND YEAR = 2013"

      var queries: Array[String] = new Array[String](7);
      queries(0) = Q1
      queries(1) = Q2
      queries(2) = Q3
      queries(3) = Q4
      queries(4) = Q5
      queries(5) = Q6
      queries(6) = Q7
      CommonObject.runQueries("SnappyConnector", pw, snc.sqlContext, queries, 1)
      start = System.currentTimeMillis
      snc.sql("update airline set ArrDelay= -15  where UniqueCarrier = 'US' AND FLIGHTNUM = 401 AND MONTH = 12 AND YEAR = 2014 AND DAYOFMONTH = 1").show()
      end = System.currentTimeMillis
      pw.println(s"\nTime taken to update arrival delay for a particular flight on a particular day = " +
          (end - start) + " ms")
      snc.sql("select ArrDelay arrivalDelay from airline where UniqueCarrier = 'US' AND FLIGHTNUM = 401 AND MONTH = 12 AND YEAR = 2014 AND DAYOFMONTH = 1").show
      start = System.currentTimeMillis
      snc.sql("update airline set WeatherDelay= 12  WHERE UniqueCarrier = 'MQ' AND FLIGHTNUM = 2718 AND DAYOFMONTH = 21 AND MONTH = 2 AND YEAR = 2013")
      end = System.currentTimeMillis
      pw.println(s"\nTime taken to update weather delay time for a particular American Eagle flight on a particular day = " +
          (end - start) + " ms")
      snc.sql("SELECT WeatherDelay FROM airline WHERE UniqueCarrier = 'MQ' AND FLIGHTNUM = 2718 AND DAYOFMONTH = 21 AND MONTH = 2 AND YEAR = 2013").show
    } match {
      case Success(v) => pw.close()
      case Failure(e) =>
        pw.println("Exception occurred while executing the job " + "\nError Message:" + e
            .getMessage)
        e.printStackTrace(pw)
        pw.close()

    }
  }
}

