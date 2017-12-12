import java.io.{File, FileOutputStream, PrintStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object LoadAndQueryPerfAlluxioSparkApp {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession
        .builder.appName("Alluxio_Airline")
        .getOrCreate

    val pw = new PrintWriter(new FileOutputStream(new File("LoadAndQueryPerfAlluxioSparkApp.out"),
      true))
    Try {
      val airlineDataFrame = CommonObject.getAirlineDataFrame(sparkSession.sqlContext)
      val airlineRefDataFrame = CommonObject.getAirlineRefDataFrame(sparkSession.sqlContext)

      var start = System.currentTimeMillis
      airlineDataFrame.write.parquet("alluxio://localhost:19998/airline")
      var end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airline = " +
          (end - start) + " ms")

      start = System.currentTimeMillis
      airlineRefDataFrame.write.parquet("alluxio://localhost:19998/airlineRef")
      end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airlineref = " +
          (end - start) + " ms")

      val Q1 = "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier from parquet.`alluxio://localhost:19998/airline` group by UniqueCarrier order by arrivalDelay"
      val Q2 = "select  count(*) flightRecCount, DESCRIPTION AirlineName, UniqueCarrier carrierCode ,Year from parquet.`alluxio://localhost:19998/airline` as airline , parquet.`alluxio://localhost:19998/airlineRef` as airlineref where airline.UniqueCarrier = airlineref.CODE group by UniqueCarrier,DESCRIPTION, Year order by flightRecCount desc limit 10"
      val Q3 = "select AVG(ArrDelay) arrivalDelay, DESCRIPTION AirlineName, UniqueCarrier carrier from parquet.`alluxio://localhost:19998/airline` as airline, parquet.`alluxio://localhost:19998/airlineRef` as airlineref where airline.UniqueCarrier = airlineref.CODE group by UniqueCarrier, DESCRIPTION order by arrivalDelay"
      val Q4 = "select AVG(ArrDelay) ArrivalDelay, Year from parquet.`alluxio://localhost:19998/airline` group by Year order by Year"
      val Q5 = "SELECT sum(WeatherDelay) totalWeatherDelay, airlineref.DESCRIPTION from parquet.`alluxio://localhost:19998/airline` as airline, parquet.`alluxio://localhost:19998/airlineRef` as airlineref WHERE airline.UniqueCarrier = airlineref.CODE AND  Origin like '%SFO%' AND WeatherDelay > 0 GROUP BY DESCRIPTION limit 20"
      val Q6 = "select ArrDelay arrivalDelay from parquet.`alluxio://localhost:19998/airline` where UniqueCarrier = 'US' AND FLIGHTNUM = 401 AND MONTH = 12 AND YEAR = 2014 AND DAYOFMONTH = 1"
      val Q7 = "select WeatherDelay from parquet.`alluxio://localhost:19998/airline` where UniqueCarrier = 'MQ' AND FLIGHTNUM = 2718 AND DAYOFMONTH = 21 AND MONTH = 2 AND YEAR = 2013"
      var queries: Array[String] = new Array[String](7);
      queries(0) = Q1
      queries(1) = Q2
      queries(2) = Q3
      queries(3) = Q4
      queries(4) = Q5
      queries(5) = Q6
      queries(6) = Q7

      CommonObject.runQueries("Alluxio", pw, sparkSession.sqlContext, queries, 1)

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