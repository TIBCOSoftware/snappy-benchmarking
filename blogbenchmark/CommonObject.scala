import java.io.{File, FileOutputStream, PrintStream, PrintWriter}

import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object CommonObject {

  def getAirlineDataFrame(sqlContext: SQLContext): DataFrame = {
    val tempAirlineDataFrame: DataFrame = sqlContext.read.format("parquet").load("/datadrive1/users/swati/perfBlog/152/1995-2015_ParquetData")
    tempAirlineDataFrame.createOrReplaceTempView("tempAirline")
    val airlineDataFrame = sqlContext.sql("select monotonically_increasing_id() as id, * from tempAirline")
    println(airlineDataFrame.schema.mkString(","))
    airlineDataFrame.na.fill(0L)
    import org.apache.spark.sql.functions._
    val safeString: String => Double = s => if (s == null || s == "") 0 else s.toDouble
    val toDouble = udf(safeString)
    airlineDataFrame.withColumn("WeatherDelay", toDouble(airlineDataFrame("WeatherDelay")))
  }

  def getAirlineRefDataFrame(sqlContext: SQLContext): DataFrame = {
    val airportCodeDataFrame: DataFrame = sqlContext.read.format("parquet").load("/datadrive1/users/swati/perfBlog/152/airportcodeParquetData")
    println(airportCodeDataFrame.schema.mkString(","))
    airportCodeDataFrame
  }

  def runQueries(productName: String, pw: PrintWriter, sqlContext: SQLContext, queries: Array[String], iteration: Int): Unit = {
    val averageFileStream: FileOutputStream = new FileOutputStream(new File(s"Average_${productName}_${iteration}"))
    val averagePrintStream: PrintStream = new PrintStream(averageFileStream)

    for (i <- 0 until queries.length) {
      var queryNumber = i + 1
      val queryFileStream: FileOutputStream = new FileOutputStream(new File(s"${queryNumber}_${productName}_${iteration}"))
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
