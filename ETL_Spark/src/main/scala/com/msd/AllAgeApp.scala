package com.msd

import java.io.File

import java.util.Properties
import java.sql._

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}

import scala.collection.mutable.ListBuffer

object AllAgeApp extends App {

  private case object HiveDialect extends JdbcDialect {
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

    override def quoteIdentifier(colName: String): String = {
      colName.split('.').map(part => s"`$part`").mkString(".")
    }
  }

  // warehouseLocation points to the default location for managed databases and tables
  val warehouseLocation = new File("/apps/spark/warehouse").getAbsolutePath

  val conf: SparkConf = new SparkConf()
    .setAppName("ETL Job")

  val sc: SparkContext = new SparkContext(conf)

  val spark: SparkSession = SparkSession.builder().config("spark.sql.warehouse.dir", warehouseLocation).config("spark.debug.maxToStringFields", 100).getOrCreate()

  import spark.sql

  JdbcDialects.registerDialect(HiveDialect)

  // Create the JDBC URL without passing in the user and password parameters.
  val jdbcUrl: String = "jdbc:hive2://sandbox-hdp.hortonworks.com:2181/msd;password=hive;serviceDiscoveryMode=zooKeeper;user=hive;zooKeeperNamespace=hiveserver2"
  val driver: String = "org.apache.hive.jdbc.HiveDriver"
  val username: String = "hive"
  val password: String = "hive"

  // Create a Properties() object to hold the parameters.
  val connectionProperties: Properties = new Properties()
  connectionProperties.setProperty("user", username)
  connectionProperties.setProperty("password", password)
  connectionProperties.setProperty("Driver", driver)
  connectionProperties.setProperty("fetchsize", "10")

  // there's probably a better way to do this
  var connection: Connection = DriverManager.getConnection(jdbcUrl, username, password)

  // create the statement, and run the select query
  val statement = connection.createStatement()

  val resultSet: ResultSet = statement.executeQuery("DESCRIBE msd.raw_csv")

  val metaData: ResultSetMetaData = resultSet.getMetaData
  var schema: ListBuffer[String] = new ListBuffer[String]()
  while ( {
    resultSet.next
  }) schema += resultSet.getString(metaData.getColumnName(1))

  val rawDF: DataFrame = spark.read.jdbc(jdbcUrl, "msd.raw_csv", connectionProperties).toDF(schema: _*)

  rawDF.createOrReplaceTempView("raw_data")
  println("Original Data")
  rawDF.show(10)

  /*
    - Average of each Question’s "Data_Value" by year for all age groups
  * */
  val sqlStatement: String =
    """
      |SELECT
      |question,
      |yearstart as year,
      |age ,
      |AVG(data_value) as avg_value
      |FROM raw_data
      |WHERE age != ''
      |GROUP BY question, yearstart, age
      |ORDER BY question, year, age
    """.stripMargin

  val yearAvgDF: DataFrame = sql(sqlStatement).toDF()
  println("Yearly Average Value for each Question and Age group")
  yearAvgDF.show()

  sql("CREATE DATABASE IF NOT EXISTS msd")
  yearAvgDF.repartition(10).write.mode(SaveMode.Overwrite).saveAsTable("msd.yearly_avg_all_age")
  println(sql("SELECT * FROM msd.yearly_avg_all_age").show(10))
}
