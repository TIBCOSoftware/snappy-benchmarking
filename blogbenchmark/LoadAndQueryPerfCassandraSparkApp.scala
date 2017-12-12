import java.io.{File, FileOutputStream, PrintStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import com.datastax.spark.connector.DataFrameFunctions
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object LoadAndQueryPerfCassandraSparkApp {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession
        .builder.appName("Cassandra_Airline")
        .config("spark.cassandra.connection.host", "localhost")
        .config("spark.cassandra.auth.username", "cassandra")
        .config("spark.cassandra.auth.password", "cassandra")
        .getOrCreate
    val sqlContext = sparkSession.sqlContext

    CassandraConnector(sparkSession.sparkContext.getConf).withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS app")
      session.execute("CREATE KEYSPACE app WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    }

    val pw = new PrintWriter(new FileOutputStream(new File("LoadAndQueryPerfCassandraSparkApp.out"),
      true));
    Try {

      val airlineCassandraOptions = Map("table" -> "airline", "keyspace" -> "app", "spark.cassandra.input.fetch.size_in_rows"->"200000", "spark.cassandra.read.timeout_ms" -> "10000")
      val airlineRefCassandraOptions = Map("table" -> "airlineref", "keyspace" -> "app","spark.cassandra.read.timeout_ms" -> "10000")


      val airlineDataFrame = CommonObject.getAirlineDataFrame(sqlContext)
      val airlineRefDataFrame = CommonObject.getAirlineRefDataFrame(sqlContext)

      airlineDataFrame.na.fill(0L)

      var airlineFrameFunctions: DataFrameFunctions = new DataFrameFunctions(airlineDataFrame)
      airlineFrameFunctions.createCassandraTable(
        "app",
        "airline" ,
        partitionKeyColumns = Some(Seq("UNIQUECARRIER")),
        clusteringKeyColumns = Some(Seq("id")))

      var start = System.currentTimeMillis
      airlineDataFrame.write.format("org.apache.spark.sql.cassandra").options(airlineCassandraOptions).save()
      var end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airline = " +
          (end - start) + " ms")

      var airlineRefFrameFunctions: DataFrameFunctions = new DataFrameFunctions(airlineRefDataFrame)
      airlineRefFrameFunctions.createCassandraTable(
        "app",
        "airlineref",
        partitionKeyColumns = Some(Seq("CODE")))

      start = System.currentTimeMillis
      airlineRefDataFrame.write.format("org.apache.spark.sql.cassandra").options(airlineRefCassandraOptions).save()
      end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airlineref = " +
          (end - start) + " ms")

      var airlinedataFrameCassandra = sqlContext.read.format("org.apache.spark.sql.cassandra")
          .options(airlineCassandraOptions)
          .load()

      var airlineRefdataFrameCassandra = sqlContext.read.format("org.apache.spark.sql.cassandra")
          .options(airlineRefCassandraOptions)
          .load()

      airlinedataFrameCassandra.createOrReplaceTempView("airlineSpark")
      airlineRefdataFrameCassandra.createOrReplaceTempView("airlinerefSpark")

      val Q1 = "select AVG(ArrDelay) arrivalDelay, UNIQUECARRIER carrier from airlineSpark group by UNIQUECARRIER order by arrivalDelay"
      val Q2 = "select  count(*) flightRecCount, DESCRIPTION AirlineName, UNIQUECARRIER carrierCode ,Year from airlineSpark , airlinerefSpark where airlineSpark.UNIQUECARRIER = airlinerefSpark.CODE group by UNIQUECARRIER,DESCRIPTION, Year order by flightRecCount desc limit 10"
      val Q3 = "select AVG(ArrDelay) arrivalDelay, DESCRIPTION AirlineName, UNIQUECARRIER carrier from airlineSpark, airlinerefSpark where airlineSpark.UNIQUECARRIER = airlinerefSpark.CODE group by UNIQUECARRIER, DESCRIPTION order by arrivalDelay"
      val Q4 = "select AVG(ArrDelay) ArrivalDelay, Year from airlineSpark group by Year order by Year"
      val Q5 = "SELECT sum(WeatherDelay) totalWeatherDelay, airlinerefSpark.DESCRIPTION FROM airlineSpark, airlinerefSpark WHERE airlineSpark.UNIQUECARRIER = airlinerefSpark.CODE AND  Origin like '%SFO%' AND WeatherDelay > 0 GROUP BY DESCRIPTION limit 20"
      val Q6 = "select ArrDelay arrivalDelay from airlineSpark where UNIQUECARRIER = 'US' AND FLIGHTNUM = 401 AND MONTH = 12 AND YEAR = 2014 AND DAYOFMONTH = 1"
      val Q7 = "select WeatherDelay from airlineSpark where UNIQUECARRIER = 'MQ' AND FLIGHTNUM = 2718 AND DAYOFMONTH = 21 AND MONTH = 2 AND YEAR = 2013"

      var queries: Array[String] = new Array[String](7);
      queries(0) = Q1
      queries(1) = Q2
      queries(2) = Q3
      queries(3) = Q4
      queries(4) = Q5
      queries(5) = Q6
      queries(6) = Q7

      CommonObject.runQueries("Cassandra", pw, sqlContext, queries, 1)

      var updateDF1:DataFrame = sqlContext.sql("select * from airlineSpark where UNIQUECARRIER = 'US' AND FLIGHTNUM = 401 AND MONTH = 12 AND YEAR = 2014 AND DAYOFMONTH = 1")
      var encoder = RowEncoder(updateDF1.schema)
      var putDF = updateDF1.map{row => Row(row(0), row(1), row(2), row(3), -15.00, row(5), row(6), row(7),
        row(8), row(9), row(10), row(11), row(12), row(13), row(14), row(15),
        row(16), row(17), row(18), row(19), row(20), row(21), row(22), row(23) ,
        row(24), row(25), row(26), row(27), row(28), row(29), row(30))}(encoder)

      var startTime = System.currentTimeMillis()
      putDF.write.format("org.apache.spark.sql.cassandra")
          .options(airlineCassandraOptions)
          .mode("append")
          .save()
      var endTime = System.currentTimeMillis()
      pw.println(s"Update1 Time : ${endTime-startTime}")

      pw.print(updateDF1.show())
      pw.print(updateDF1.count)
      pw.print(updateDF1.schema)

      updateDF1 = sqlContext.sql("select ArrDelay from airlineSpark where UNIQUECARRIER = 'US' AND FLIGHTNUM = 401 AND MONTH = 12 AND YEAR = 2014 AND DAYOFMONTH = 1")
      pw.print(updateDF1.show())

      var updateDF2:DataFrame = sqlContext.sql("select * from airlineSpark WHERE UNIQUECARRIER = 'MQ' AND FLIGHTNUM = 2718 AND DAYOFMONTH = 21 AND MONTH = 2 AND YEAR = 2013")
      encoder = RowEncoder(updateDF2.schema)
      putDF = updateDF2.map{row => Row(row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7),
        row(8), row(9), row(10), row(11), row(12), row(13), row(14), row(15),
        row(16), row(17), row(18), row(19), row(20), row(21), row(22), row(23),
        row(24), row(25), row(26), row(27), row(28), 12.00, row(30))}(encoder)

      startTime = System.currentTimeMillis()
      putDF.write.format("org.apache.spark.sql.cassandra")
          .options(airlineCassandraOptions)
          .mode("append")
          .save()
      endTime = System.currentTimeMillis()
      pw.println(s"Update2 Time : ${endTime-startTime}")

      pw.print(updateDF2.show())
      pw.print(updateDF2.count)
      updateDF2 = sqlContext.sql("select WeatherDelay from airlineSpark WHERE UNIQUECARRIER = 'MQ' AND FLIGHTNUM = 2718 AND DAYOFMONTH = 21 AND MONTH = 2 AND YEAR = 2013")
      pw.print(updateDF2.show())

      //CommonObject.runQueries("Cassandra", pw, sqlContext, queries, 2)
    } match {
      case Success(v) => pw.close()
      case Failure(e) =>
        e.printStackTrace(pw)
        pw.println("Exception occurred while executing the job " +
            "\nError Message:" + e.getMessage)
        pw.close()
    }
  }
}