package com.dataplatform.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataTransformer(spark: SparkSession) {
  
  /**
   * Execute custom SQL transformation
   */
  def executeSQL(df: DataFrame, sqlQuery: String, tempViewName: String = "temp_table"): DataFrame = {
    
    println(s"Executing SQL transformation on view: $tempViewName")
    
    // Create temporary view
    df.createOrReplaceTempView(tempViewName)
    
    // Execute SQL
    spark.sql(sqlQuery)
  }
  
  /**
   * Add standard metadata columns
   */
  def addMetadata(df: DataFrame): DataFrame = {
    df.withColumn("_ingestion_timestamp", current_timestamp())
      .withColumn("_processing_date", current_date())
      .withColumn("_source_system", lit("backend_service"))
  }
  
  /**
   * Clean and standardize column names
   */
  def standardizeColumns(df: DataFrame): DataFrame = {
    val standardizedColumns = df.columns.map { col =>
      val newName = col.toLowerCase
        .replaceAll("[\\s-]+", "_")
        .replaceAll("[^a-z0-9_]", "")
      df.col(col).alias(newName)
    }
    
    df.select(standardizedColumns: _*)
  }
  
  /**
   * Apply simple column transformations
   */
  def applyTransformations(df: DataFrame, transformations: Map[String, String]): DataFrame = {
    var result = df
    
    transformations.foreach { case (column, expression) =>
      println(s"Applying transformation to column '$column': $expression")
      result = result.withColumn(column, expr(expression))
    }
    
    result
  }
}
