package com.dataplatform

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.dataplatform.core.{DataConnector, DataTransformer}

/**
 * Main Backend Service for Data Platform POC
 */
class BackendService(spark: SparkSession) {
  
  private val dataConnector = new DataConnector(spark)
  private val dataTransformer = new DataTransformer(spark)
  
  /**
   * Extract data from Oracle
   */
  def extractFromOracle(
    projectId: String,
    secretId: String,
    query: String
  ): DataFrame = {
    dataConnector.connectOracle(projectId, secretId, query)
  }
  
  /**
   * Extract data from GCS
   */
  def extractFromGCS(
    filePath: String,
    fileFormat: String = "csv",
    header: Boolean = true
  ): DataFrame = {
    dataConnector.readFromGCS(filePath, fileFormat, header)
  }
  
  /**
   * Transform data with SQL
   */
  def transformWithSQL(
    df: DataFrame,
    sqlQuery: String,
    tempViewName: String = "source_data"
  ): DataFrame = {
    dataTransformer.executeSQL(df, sqlQuery, tempViewName)
  }
  
  /**
   * Apply simple transformations
   */
  def applyTransformations(
    df: DataFrame,
    transformations: Map[String, String]
  ): DataFrame = {
    dataTransformer.applyTransformations(df, transformations)
  }
  
  /**
   * Add metadata and standardize
   */
  def standardizeData(df: DataFrame): DataFrame = {
    val withMetadata = dataTransformer.addMetadata(df)
    dataTransformer.standardizeColumns(withMetadata)
  }
  
  /**
   * Save data to GCS
   */
  def saveToGCS(
    df: DataFrame,
    outputPath: String,
    fileFormat: String = "parquet",
    mode: String = "overwrite"
  ): Unit = {
    dataConnector.writeToGCS(df, outputPath, fileFormat, mode)
  }
  
  /**
   * Complete pipeline: Extract -> Transform -> Load
   */
  def runPipeline(
    sourceType: String,
    sourceConfig: Map[String, String],
    transformConfig: Map[String, String],
    targetConfig: Map[String, String]
  ): Unit = {
    
    println("=== Starting Data Pipeline ===")
    
    // 1. Extract
    println("1. Extracting data...")
    val sourceData = sourceType.toLowerCase match {
      case "oracle" =>
        extractFromOracle(
          sourceConfig("project_id"),
          sourceConfig("secret_id"),
          sourceConfig("query")
        )
      case "gcs" =>
        extractFromGCS(
          sourceConfig("file_path"),
          sourceConfig.getOrElse("file_format", "csv"),
          sourceConfig.getOrElse("header", "true").toBoolean
        )
      case _ =>
        throw new IllegalArgumentException(s"Unsupported source type: $sourceType")
    }
    
    println(s"Extracted ${sourceData.count()} records")
    
    // 2. Transform
    println("2. Transforming data...")
    var transformedData = sourceData
    
    // Apply SQL transformation if provided
    if (transformConfig.contains("sql_query")) {
      transformedData = transformWithSQL(
        transformedData,
        transformConfig("sql_query"),
        transformConfig.getOrElse("temp_view", "source_data")
      )
    }
    
    // Apply column transformations if provided
    val columnTransformations = transformConfig.filterKeys(_.startsWith("transform_"))
      .map { case (k, v) => k.replace("transform_", "") -> v }
    
    if (columnTransformations.nonEmpty) {
      transformedData = applyTransformations(transformedData, columnTransformations)
    }
    
    // Add metadata
    transformedData = standardizeData(transformedData)
    
    println(s"Transformed data has ${transformedData.count()} records")
    
    // 3. Load
    println("3. Loading data...")
    saveToGCS(
      transformedData,
      targetConfig("output_path"),
      targetConfig.getOrElse("file_format", "parquet"),
      targetConfig.getOrElse("mode", "overwrite")
    )
    
    println("=== Pipeline completed successfully ===")
  }
}
