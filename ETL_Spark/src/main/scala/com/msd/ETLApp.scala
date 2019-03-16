package com.msd

import java.util.Properties
import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSetMetaData

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}

import scala.collection.mutable.ListBuffer

object ETLApp extends App {

  private case object HiveDialect extends JdbcDialect {
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

    override def quoteIdentifier(colName: String): String = {
      colName.split('.').map(part => s"`$part`").mkString(".")
    }
  }

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("ETL Job")

  val sc = new SparkContext(conf)

  val spark = SparkSession
    .builder()
    .config("hive.resultset.use.unique.column.names", "false")
    .getOrCreate()

  JdbcDialects.registerDialect(HiveDialect)

  // Create the JDBC URL without passing in the user and password parameters.
  val jdbcUrl = s"jdbc:hive2://localhost:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
  val driver = "org.apache.hive.jdbc.HiveDriver"
  val username = "hive"
  val password = "hive"

  // Create a Properties() object to hold the parameters.
  val connectionProperties = new Properties()
  connectionProperties.setProperty("user", username)
  connectionProperties.setProperty("password", password)
  connectionProperties.setProperty("Driver", driver)

  // there's probably a better way to do this
  var connection: Connection = DriverManager.getConnection(jdbcUrl, username, password)

  // create the statement, and run the select query
  val statement = connection.createStatement()

  val resultSet = statement.executeQuery("DESCRIBE msd.raw_orc")

  val metaData: ResultSetMetaData = resultSet.getMetaData
  var schema = new ListBuffer[String]()
  while ( {
    resultSet.next
  }) schema += resultSet.getString(metaData.getColumnName(1))

  val employees_table = spark.read.jdbc(jdbcUrl, "msd.raw_orc", connectionProperties)

  val df = employees_table.toDF(schema: _*)

  println(df.select("yearstart", "yearend").show(2))


}
