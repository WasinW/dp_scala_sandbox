package com.dataplatform.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties

class DataConnector(spark: SparkSession) {
  
  /**
   * Connect to Oracle database and execute query
   */
  def connectOracle(
    projectId: String,
    secretId: String,
    query: String
  ): DataFrame = {
    
    val credentials = SecretManager.getDatabaseCredentials(projectId, secretId)
    
    val connectionProperties = new Properties()
    connectionProperties.put("user", credentials.username)
    connectionProperties.put("password", credentials.password)
    connectionProperties.put("driver", credentials.driver)
    connectionProperties.put("fetchsize", "10000")
    
    // Oracle specific optimizations
    connectionProperties.put("oracle.jdbc.timezoneAsRegion", "false")
    connectionProperties.put("oracle.net.disableOob", "true")
    
    println(s"Connecting to Oracle: ${credentials.url}")
    
    spark.read
      .jdbc(credentials.url, s"($query) as subquery", connectionProperties)
  }
  
  /**
   * Read files from GCS
   */
  def readFromGCS(
    filePath: String,
    fileFormat: String = "csv",
    header: Boolean = true
  ): DataFrame = {
    
    println(s"Reading from GCS: $filePath")
    
    fileFormat.toLowerCase match {
      case "csv" =>
        spark.read
          .option("header", header.toString)
          .option("inferSchema", "true")
          .option("multiline", "true")
          .option("escape", "\"")
          .csv(filePath)
          
      case "json" =>
        spark.read
          .option("multiline", "true")
          .json(filePath)
          
      case "parquet" =>
        spark.read.parquet(filePath)
        
      case _ =>
        throw new IllegalArgumentException(s"Unsupported file format: $fileFormat")
    }
  }
  
  /**
   * Write DataFrame to GCS
   */
  def writeToGCS(
    df: DataFrame,
    outputPath: String,
    fileFormat: String = "parquet",
    mode: String = "overwrite"
  ): Unit = {
    
    println(s"Writing to GCS: $outputPath")
    
    fileFormat.toLowerCase match {
      case "csv" =>
        df.write
          .mode(mode)
          .option("header", "true")
          .csv(outputPath)
          
      case "json" =>
        df.write
          .mode(mode)
          .json(outputPath)
          
      case "parquet" =>
        df.write
          .mode(mode)
          .parquet(outputPath)
          
      case _ =>
        throw new IllegalArgumentException(s"Unsupported file format: $fileFormat")
    }
  }
}
