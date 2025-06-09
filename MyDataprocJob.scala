package com.dataplatform

import org.apache.spark.sql.SparkSession

/**
 * Main entry point for Dataproc job
 */
object MyDataprocJob {
  
  def main(args: Array[String]): Unit = {
    
    // Parse arguments
    val pipelineScript = getArgValue(args, "--pipeline-script", "")
    
    if (pipelineScript.isEmpty) {
      println("Error: --pipeline-script parameter is required")
      System.exit(1)
    }
    
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("DataPlatform-Backend-POC")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    
    try {
      println(s"Starting job with pipeline script: $pipelineScript")
      
      // Initialize backend service
      val backendService = new BackendService(spark)
      
      // Execute pipeline script (simplified - in real implementation would load from GCS)
      executePipelineScript(backendService, pipelineScript)
      
    } catch {
      case e: Exception =>
        println(s"Job failed: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
  
  private def executePipelineScript(
    backendService: BackendService,
    scriptPath: String
  ): Unit = {
    
    // For POC, we'll execute a sample pipeline
    // In real implementation, you would load and parse the notebook/script
    
    println("Executing sample pipeline...")
    
    // Example Oracle to GCS pipeline
    val sourceConfig = Map(
      "project_id" -> sys.env.getOrElse("GOOGLE_CLOUD_PROJECT", "your-project"),
      "secret_id" -> "oracle-db-credentials",
      "query" -> "SELECT customer_id, name, email, created_date FROM customers WHERE rownum <= 1000"
    )
    
    val transformConfig = Map(
      "sql_query" -> """
        SELECT 
          customer_id,
          UPPER(TRIM(name)) as customer_name,
          LOWER(TRIM(email)) as email,
          created_date,
          CASE 
            WHEN created_date >= current_date() - INTERVAL 30 DAY THEN 'NEW'
            ELSE 'EXISTING'
          END as customer_type
        FROM source_data
      """,
      "transform_email" -> "regexp_replace(email, ' ', '')"
    )
    
    val targetConfig = Map(
      "output_path" -> "gs://your-bucket/output/customers",
      "file_format" -> "parquet",
      "mode" -> "overwrite"
    )
    
    backendService.runPipeline("oracle", sourceConfig, transformConfig, targetConfig)
  }
  
  private def getArgValue(args: Array[String], key: String, default: String): String = {
    args.find(_.startsWith(s"$key="))
      .map(_.split("=", 2)(1))
      .getOrElse(default)
  }
}