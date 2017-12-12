import java.io.{File, FileOutputStream, PrintStream, PrintWriter}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import org.apache.kudu.client
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._


object LoadAndQueryPerfKuduSparkApp {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession
        .builder
        .appName("Kudu_Spark")
        .getOrCreate
    val sqlContext = sparkSession.sqlContext

    val pw = new PrintWriter(new FileOutputStream(new File("LoadAndQueryPerfKuduSparkApp.out"),
      true))
    Try {
      val master1 = "127.0.0.1:7051"
      val kuduMasters = Seq(master1).mkString(",")

      val kuduContext = new KuduContext(kuduMasters, sqlContext.sparkContext)

      var kuduAirlineTableName = "airline"
      var kuduAirportTableName = "airlineref"

      if (kuduContext.tableExists(kuduAirlineTableName)) {
        kuduContext.deleteTable(kuduAirlineTableName)
      }

      if (kuduContext.tableExists(kuduAirportTableName)) {
        kuduContext.deleteTable(kuduAirportTableName)
      }

      val kuduAirlinePrimaryKey = Seq("id","UNIQUECARRIER")
      val kuduAirportPrimaryKey = Seq("CODE")

      //val kuduAirlineTableOptions = new CreateTableOptions().setRangePartitionColumns(List("UniqueCarrier").asJava)
      val kuduAirlineTableOptions = new client.CreateTableOptions().addHashPartitions(List("UNIQUECARRIER").asJava, 20).setNumReplicas(1)
      val kuduAirportTableOptions = new CreateTableOptions().addHashPartitions(List("CODE").asJava, 20).setNumReplicas(1)


      // Define Kudu options used by various operations
      val kuduAirlineOptions: Map[String, String] = Map(
        "kudu.table" -> kuduAirlineTableName,
        "kudu.master" -> kuduMasters)

      val kuduAirportOptions: Map[String, String] = Map(
        "kudu.table" -> kuduAirportTableName,
        "kudu.master" -> kuduMasters)

      val airlineDataFrame = CommonObject.getAirlineDataFrame(sqlContext)
      val airlineRefDataFrame = CommonObject.getAirlineRefDataFrame(sqlContext)

      var kuduAirlineTableSchema = airlineDataFrame.schema
      var kuduAirportTableSchema = airlineRefDataFrame.schema

      kuduContext.createTable(kuduAirlineTableName, kuduAirlineTableSchema, kuduAirlinePrimaryKey, kuduAirlineTableOptions)
      kuduContext.createTable(kuduAirportTableName, kuduAirportTableSchema, kuduAirportPrimaryKey, kuduAirportTableOptions)

      // 2. Insert our customer DataFrame data set into the Kudu table
      var start = System.currentTimeMillis
      kuduContext.insertRows(airlineDataFrame, kuduAirlineTableName)
      var end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airline = " +
          (end - start) + " ms")

      start = System.currentTimeMillis
      kuduContext.insertRows(airlineRefDataFrame, kuduAirportTableName)
      end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airlineref = " +
          (end - start) + " ms")

      sqlContext.read.options(kuduAirlineOptions).kudu.createOrReplaceTempView(kuduAirlineTableName)
      sqlContext.read.options(kuduAirportOptions).kudu.createOrReplaceTempView(kuduAirportTableName)


      airlineDataFrame.createOrReplaceTempView("SparkAirline")
      airlineRefDataFrame.createOrReplaceTempView("SparkAirlineRef")

      val Q1 = "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier from airline group by UniqueCarrier order by arrivalDelay"
      val Q2 = "select  count(*) flightRecCount, DESCRIPTION AirlineName, UNIQUECARRIER carrierCode ,Year from airline , airlineref where airline.UniqueCarrier = airlineref.CODE group by UniqueCarrier,DESCRIPTION, Year order by flightRecCount desc limit 10"
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

      CommonObject.runQueries("Kudu", pw, sqlContext, queries, 1)

      var updateDF1:DataFrame = sqlContext.sql("select * from SparkAirline where UNIQUECARRIER = 'US' AND FLIGHTNUM = 401 AND MONTH = 12 AND YEAR = 2014 AND DAYOFMONTH = 1")
      pw.print(updateDF1.show())
      pw.print(updateDF1.count)
      var encoder = RowEncoder(updateDF1.schema)

      var putDF = updateDF1.map{row => Row(row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7),
        row(8), row(9), row(10), row(11), row(12), row(13), row(14), row(15),
        row(16), -15.00, row(18), row(19), row(20), row(21), row(22), row(23),
        row(24), row(25), row(26), row(27), row(28), row(29), row(30))}(encoder)

      start =  System.currentTimeMillis()
      kuduContext.updateRows(putDF, kuduAirlineTableName)
      end =  System.currentTimeMillis()
      pw.println(s" Update1 Time : ${end - start}")

      updateDF1 = sqlContext.sql("select ArrDelay from airline where UNIQUECARRIER = 'US' AND FLIGHTNUM = 401 AND MONTH = 12 AND YEAR = 2014 AND DAYOFMONTH = 1")
      pw.print(updateDF1.show())

      var updateDF2:DataFrame = sqlContext.sql("select * from SparkAirline where UNIQUECARRIER = 'MQ' AND FLIGHTNUM = 2718 AND DAYOFMONTH = 21 AND MONTH = 2 AND YEAR = 2013")
      pw.print(updateDF2.show())
      pw.print(updateDF2.count)
      encoder = RowEncoder(updateDF2.schema)
      putDF = updateDF2.map{row => Row(row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7),
        row(8), row(9), row(10), row(11), row(12), row(13), row(14), row(15),
        row(16), row(17), row(18), row(19), row(20), row(21), row(22), row(23),
        row(24), row(25), 12.00, row(27), row(28), row(29), row(30))}(encoder)
      start =  System.currentTimeMillis()
      kuduContext.updateRows(putDF, kuduAirlineTableName)
      end =  System.currentTimeMillis()
      pw.println(s" Update2 Time : ${end - start}")

      updateDF2 = sqlContext.sql("select WeatherDelay from airline where UNIQUECARRIER = 'MQ' AND FLIGHTNUM = 2718 AND DAYOFMONTH = 21 AND MONTH = 2 AND YEAR = 2013")
      pw.print(updateDF2.show())

      CommonObject.runQueries("Kudu", pw, sqlContext, queries, 2)

      /*sqlContext.sql("select count(*) from airline where UNIQUECARRIER='DL'").show()
      var deleteKeysDF = sqlContext.sql("select id, UNIQUECARRIER from airline where UNIQUECARRIER='DL'")
      deleteKeysDF.show

      start =  System.currentTimeMillis()
      kuduContext.deleteRows(deleteKeysDF, kuduAirlineTableName)
      end =  System.currentTimeMillis()
      pw.println(s" Delete1 Time : ${end - start}")

      sqlContext.sql("select count(*) from airline where UNIQUECARRIER='DL'").show()

      sqlContext.sql("select count(*) from airline where Year=2017").show()
      deleteKeysDF = sqlContext.sql("select id, UNIQUECARRIER from airline where Year=2015")
      deleteKeysDF.show

      start =  System.currentTimeMillis()
      kuduContext.deleteRows(deleteKeysDF, kuduAirlineTableName)
      end =  System.currentTimeMillis()
      pw.println(s" Delete2 Time : ${end - start}")

      sqlContext.sql("select count(*) from airline where Year=2017").show()

      //CommonObject.runQueries("Kudu", pw, sqlContext, queries, 3)*/

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