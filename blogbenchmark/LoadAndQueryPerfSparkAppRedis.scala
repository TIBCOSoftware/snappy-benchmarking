import java.io.{File, FileOutputStream, PrintStream, PrintWriter}

import com.redislabs.provider.redis._

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.{Failure, Success, Try}


object LoadAndQueryPerfSparkAppRedis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LoadAndQueryPerfRedisSparkApp").
        set("redis.host", "127.0.0.1").set("redis.port", "6379")
    val sc = SparkContext.getOrCreate(conf)
    val redisConfig = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379))

    // Flush all the hosts
    redisConfig.hosts.foreach(node => {
      val conn = node.connect
      conn.flushAll
      conn.close
    })
    val sqlContext = new SQLContext(sc)
    val pw = new PrintWriter(new FileOutputStream(new File("LoadAndQueryPerfSparkRedisApp.out"),
      true));
    Try {
      // scalastyle:off println
      val airlineDataFrame: DataFrame = sqlContext.read.load("/datadrive/users/swati/perfBlog/152/1995-2015_ParquetData") // 152 million azure
      /*sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE airline
           |(Year INT,Month INT,DayOfMonth INT,DayOfWeek INT,DepTime INT,CRSDepTime INT,ArrTime INT,CRSArrTime INT,UniqueCarrier VARCHAR(20),FlightNum INT,TailNum VARCHAR(20),ActualElapsedTime INT, CRSElapsedTime INT,AirTime INT,ArrDelay INT,DepDelay INT,Origin VARCHAR(20),Dest VARCHAR(20),Distance INT,TaxiIn INT,TaxiOut INT,Cancelled INT,CancellationCode VARCHAR(20),Diverted INT,CarrierDelay INT,WeatherDelay INT,NASDelay INT,SecurityDelay INT,LateAircraftDelay INT,ArrDelaySlot INT)
           |USING com.redislabs.provider.redis.sql
           |OPTIONS (table 'airline')
      """.stripMargin
      )*/
      sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE airline
           |(Year INT, Month INT,DayOfMonth INT,DayOfWeek INT ,UniqueCarrier VARCHAR(20) ,TailNum VARCHAR(20), FlightNum INT,Origin VARCHAR(20),Dest VARCHAR(20),CRSDepTime INT, DepTime INT,DepDelay INT,TaxiOut INT,TaxiIn INT,CRSArrTime VARCHAR(20),ArrTime VARCHAR(20),ArrDelay INT,Cancelled VARCHAR(20),CancellationCode VARCHAR(20),Diverted INT,CRSElapsedTime INT,ActualElapsedTime INT,AirTime INT,Distance INT,CarrierDelay VARCHAR(20),WeatherDelay VARCHAR(20),NASDelay VARCHAR(20),SecurityDelay VARCHAR(20),  LateAircraftDelay VARCHAR(20),ArrDelaySlot VARCHAR(20))
           |USING com.redislabs.provider.redis.sql
           |OPTIONS (table 'airline')
      """.stripMargin
      )
      // val newDf = airlineDataFrame.na.replace(airlineDataFrame.columns, Map("" -> "0"))
      // val filtered = airlineDataFrame.filter((row) => !row.anyNull)
      val newDf = airlineDataFrame.na.fill(0L)
      var start = System.currentTimeMillis
      newDf.write.insertInto("airline")
      var end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airline = " +
          (end - start) + " ms")

      // scalastyle:off println
      val airlineRefDataFrame: DataFrame = sqlContext.read.load(
        "/datadrive/users/swati/snappydata/examples/quickstart/data/airportcodeParquetData")
      sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE airlineref
           |(code VARCHAR(20),description VARCHAR(50))
           |USING com.redislabs.provider.redis.sql
           |OPTIONS (table 'airlineref')
      """.stripMargin)
      val refDf = airlineRefDataFrame.na.replace(airlineRefDataFrame.columns, Map("" -> "0"))
      start = System.currentTimeMillis
      refDf.write.insertInto("airlineref")
      end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airlineref = " +
          (end - start) + " ms")
      runQueries(pw, sqlContext)
    } match {
      case Success(v) => pw.close()
      case Failure(e) =>
        pw.println("Exception occurred while executing the job " +
            "\nError Message:" + e.getMessage)
        pw.close()
    }
  }

  // Method for running all olap and oltp queries and calculating query execution time for each
  // query


  def runQueries(pw: PrintWriter, sqlContext: SQLContext): Unit = {
    val averageFileStream: FileOutputStream = new FileOutputStream(new File(s"Average_Redis"))
    val averagePrintStream: PrintStream = new PrintStream(averageFileStream)

    // scalastyle:off println
    val Q1 = "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier from airline group by UniqueCarrier order by arrivalDelay"
    // scalastyle:off println
    val Q2 = "select  count(*) flightRecCount, DESCRIPTION AirlineName, UniqueCarrier carrierCode ,Year from airline , airlineref where airline.UniqueCarrier = airlineref.CODE group by UniqueCarrier,DESCRIPTION, Year order by flightRecCount desc limit 10"
    // scalastyle:off println
    val Q3 = "select AVG(ArrDelay) arrivalDelay, DESCRIPTION AirlineName, UniqueCarrier carrier from airline, airlineref where airline.UniqueCarrier = airlineref.CODE group by UniqueCarrier, DESCRIPTION order by arrivalDelay"
    val Q4 = "select AVG(ArrDelay) ArrivalDelay, Year from airline group by Year order by Year"
    // scalastyle:off println
    val Q5 = "SELECT sum(WeatherDelay) totalWeatherDelay, airlineref.DESCRIPTION FROM airline, airlineref WHERE airline.UniqueCarrier = airlineref.CODE AND  Origin like '%SFO%' AND WeatherDelay > 0 GROUP BY DESCRIPTION limit 20"


    var queries: Array[String] = new Array[String](5);
    queries(0) = Q1
    queries(1) = Q2
    queries(2) = Q3
    queries(3) = Q4
    queries(4) = Q5

    var isResultCollection = true

    for (i <- 0 until queries.length) {
      var queryNumber = i + 1
      val queryFileStream: FileOutputStream = new FileOutputStream(new File(s"${queryNumber}_Redis"))
      val queryPrintStream: PrintStream = new PrintStream(queryFileStream)

      // if (isResultCollection) {
      // queryPrintStream.println(queryToBeExecuted)
      var cnts: Array[Row] = sqlContext.sql(queries(i)).collect()
      pw.println(s"$queryNumber : ${cnts.length}")

      for (row <- cnts) {
        queryPrintStream.println(row.toSeq.map {
          case d: Double => "%18.4f".format(d).trim()
          case v => v
        }.mkString(","))
      }
      pw.println(s"$queryNumber Result Collected in file $queryNumber")
      // } else {
      var totalTime: Long = 0
      for (j <- 1 to 10) {
        val startTime = System.currentTimeMillis()
        var cnts: Array[Row] = sqlContext.sql(queries(i)).collect()

        for (s <- cnts) {
          // just iterating over result
        }
        val endTime = System.currentTimeMillis()
        val iterationTime = endTime - startTime
        queryPrintStream.println(s"$j, $iterationTime")
        if (j > 5) {
          totalTime += iterationTime
        }
        cnts = null
      }
      queryPrintStream.println(s"$queryNumber,${totalTime / 5}")
      averagePrintStream.println(s"$queryNumber,${totalTime / 5}")
      // }
    }
  }
}