package com.dataplatform

import org.apache.spark.sql.SparkSession
import scala.io.Source
import scala.util.{Try, Success, Failure}

/**
 * Pure Backend Service - Infrastructure Operations Only
 * No business logic - just provides data platform capabilities
 */
object MyDataprocJob {
  
  def main(args: Array[String]): Unit = {
    
    // Parse arguments
    val frontendScriptPath = getArgValue(args, "--frontend-script", "")
    val pipelineType = getArgValue(args, "--pipeline-type", "")
    val operation = getArgValue(args, "--operation", "run-frontend") // New: operation mode
    
    if (frontendScriptPath.isEmpty && pipelineType.isEmpty && operation != "test-connection") {
      println("Error: Specify --frontend-script, --pipeline-type, or --operation=test-connection")
      showUsage()
      System.exit(1)
    }
    
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("DataPlatform-Backend-Service")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()
    
    try {
      println("=== DataPlatform Backend Service Started ===")
      
      // Initialize backend service
      val backendService = new BackendService(spark)
      
      // Route to appropriate operation
      operation match {
        case "test-connection" => 
          testDatabaseConnection(backendService, args)
        case "run-frontend" => 
          runFrontendScript(backendService, frontendScriptPath, pipelineType, args)
        case "health-check" =>
          performHealthCheck(backendService)
        case _ =>
          runFrontendScript(backendService, frontendScriptPath, pipelineType, args)
      }
      
      println("=== Backend Service Completed Successfully ===")
      
    } catch {
      case e: Exception =>
        println(s"Backend service failed: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
  
  /**
   * Test database connection using mock data structure
   */
  private def testDatabaseConnection(backendService: BackendService, args: Array[String]): Unit = {
    println("\n=== Testing Database Connection ===")
    
    val projectId = getArgValue(args, "--project-id", "ntt-test-data-bq-looker")
    val testQueries = Map(
      "employees" -> "SELECT TOP 5 name, department, email FROM employees",
      "customers" -> "SELECT TOP 3 first_name, last_name, email FROM customers", 
      "products" -> "SELECT TOP 3 product_name, category, price FROM products",
      "sales" -> "SELECT TOP 3 employee_id, product_name, total_amount FROM sales"
    )
    
    testQueries.foreach { case (tableName, query) =>
      try {
        println(s"\n--- Testing table: $tableName ---")
        
        val data = backendService.extractFromOracle(
          projectId = projectId,
          secretId = "user_dev_db",
          query = query
        )
        
        val count = data.count()
        println(s"✅ Table '$tableName': Found $count records")
        
        if (count > 0) {
          data.show(5, truncate = false)
        }
        
      } catch {
        case e: Exception =>
          println(s"❌ Table '$tableName' failed: ${e.getMessage}")
      }
    }
    
    // Test write capability
    testWriteCapability(backendService)
  }
  
  /**
   * Test write capability to GCS
   */
  private def testWriteCapability(backendService: BackendService): Unit = {
    println("\n--- Testing Write Capability ---")
    
    try {
      import org.apache.spark.sql.SparkSession
      val spark = SparkSession.active
      import spark.implicits._
      
      val testData = Seq(
        ("test-connection", "2024-06-09", "SUCCESS", System.currentTimeMillis()),
        ("backend-service", "2024-06-09", "ACTIVE", System.currentTimeMillis())
      ).toDF("component", "test_date", "status", "timestamp")
      
      val outputPath = "gs://dataproc-staging-asia-southeast1-434326790648-gcyeg1qw/output/connection-test"
      
      backendService.saveToGCS(
        df = testData,
        outputPath = outputPath,
        fileFormat = "parquet",
        mode = "overwrite"
      )
      
      println(s"✅ Write test successful: $outputPath")
      
    } catch {
      case e: Exception =>
        println(s"❌ Write test failed: ${e.getMessage}")
    }
  }
  
  /**
   * Run frontend script with backend service support
   */
  private def runFrontendScript(
    backendService: BackendService,
    scriptPath: String,
    pipelineType: String,
    args: Array[String]
  ): Unit = {
    
    if (scriptPath.nonEmpty) {
      println(s"Loading frontend script from: $scriptPath")
      // Download script for reference (frontend defines the logic)
      downloadScriptFromGCS(scriptPath)
    }
    
    val actualPipelineType = if (pipelineType.nonEmpty) pipelineType else "oracle-to-gcs"
    println(s"Executing pipeline type: $actualPipelineType")
    
    // Make backend service available to frontend logic
    executeFrontendPipelineType(backendService, actualPipelineType, args)
  }
  
  /**
   * Execute frontend-defined pipeline types
   * Backend just provides the infrastructure - frontend defines business logic
   */
  private def executeFrontendPipelineType(
    backendService: BackendService,
    pipelineType: String,
    args: Array[String]
  ): Unit = {
    
    val projectId = getArgValue(args, "--project-id", "ntt-test-data-bq-looker")
    val executionDate = getArgValue(args, "--execution-date", "2024-06-09")
    
    println(s"Backend providing infrastructure for: $pipelineType")
    println(s"Project ID: $projectId")
    println(s"Execution Date: $executionDate")
    
    // Import frontend pipeline definitions
    import com.dataplatform.frontend.FrontendPipelines
    
    // Execute frontend-defined logic
    pipelineType match {
      case "test-connection" => 
        FrontendPipelines.testDatabaseConnection(backendService, projectId)
        
      case "oracle-to-gcs" => 
        FrontendPipelines.runOracleToGCS(backendService, projectId, executionDate)
        
      case "employee-analytics" => 
        FrontendPipelines.runEmployeeAnalytics(backendService, projectId, executionDate)
        
      case "sales-analytics" => 
        FrontendPipelines.runSalesAnalytics(backendService, projectId, executionDate)
        
      case "customer-analytics" => 
        FrontendPipelines.runCustomerAnalytics(backendService, projectId, executionDate)
        
      case "department-report" => 
        FrontendPipelines.runDepartmentReport(backendService, projectId, executionDate)
        
      case "sample-data-processing" => 
        FrontendPipelines.runSampleDataProcessing(backendService, executionDate)
        
      case _ =>
        println(s"⚠️  Unknown pipeline type: $pipelineType")
        println("Available types: test-connection, oracle-to-gcs, employee-analytics, sales-analytics, customer-analytics, department-report, sample-data-processing")
        System.exit(1)
    }
  }
  
  /**
   * Perform system health check
   */
  private def performHealthCheck(backendService: BackendService): Unit = {
    println("\n=== System Health Check ===")
    
    // Check Spark
    val spark = SparkSession.active
    println(s"✅ Spark Session: ${spark.sparkContext.applicationId}")
    println(s"✅ Spark Version: ${spark.version}")
    
    // Check GCS connectivity
    try {
      import spark.implicits._
      val healthData = Seq(("health-check", System.currentTimeMillis())).toDF("type", "timestamp")
      
      backendService.saveToGCS(
        df = healthData,
        outputPath = "gs://dataproc-staging-asia-southeast1-434326790648-gcyeg1qw/output/health-check",
        fileFormat = "parquet",
        mode = "overwrite"
      )
      println("✅ GCS Connectivity: OK")
      
    } catch {
      case e: Exception =>
        println(s"❌ GCS Connectivity: ${e.getMessage}")
    }
    
    // Check backend service components
    try {
      val testResult = backendService != null
      println(s"✅ Backend Service: ${if (testResult) "Initialized" else "Failed"}")
    } catch {
      case e: Exception =>
        println(s"❌ Backend Service: ${e.getMessage}")
    }
  }
  
  /**
   * Download script from GCS (for reference)
   */
  private def downloadScriptFromGCS(gcsPath: String): String = {
    import sys.process._
    
    val localPath = s"/tmp/frontend_script_${System.currentTimeMillis()}.scala"
    val downloadCmd = s"gsutil cp $gcsPath $localPath"
    
    try {
      val exitCode = downloadCmd.!
      if (exitCode == 0) {
        println(s"✅ Downloaded frontend script to: $localPath")
        localPath
      } else {
        println(s"⚠️  Could not download script from $gcsPath (exit code: $exitCode)")
        ""
      }
    } catch {
      case e: Exception =>
        println(s"⚠️  Script download failed: ${e.getMessage}")
        ""
    }
  }
  
  private def getArgValue(args: Array[String], key: String, default: String): String = {
    args.find(_.startsWith(s"$key="))
      .map(_.split("=", 2)(1))
      .getOrElse(default)
  }
  
  private def showUsage(): Unit = {
    println("""
=== DataPlatform Backend Service ===

Usage:
  spark-submit jar [options]

Operations:
  --operation=test-connection       Test database connectivity only
  --operation=health-check         Perform system health check
  --operation=run-frontend         Run frontend pipeline (default)

Frontend Pipeline Mode:
  --frontend-script=GCS_PATH       Path to frontend script (optional)
  --pipeline-type=TYPE             Pipeline type to execute
  --project-id=PROJECT             GCP Project ID  
  --execution-date=DATE            Execution date (YYYY-MM-DD)

Available Pipeline Types:
  test-connection                  Test database tables
  oracle-to-gcs                   Simple data extraction  
  employee-analytics              Employee performance analysis
  sales-analytics                 Sales performance metrics
  customer-analytics              Customer behavior analysis
  department-report               Department budget vs costs
  sample-data-processing          Process sample/mock data

Examples:
  # Test database connection
  --operation=test-connection --project-id=ntt-test-data-bq-looker
  
  # Run employee analytics
  --pipeline-type=employee-analytics --project-id=ntt-test-data-bq-looker --execution-date=2024-06-09
  
  # Run with external frontend script
  --frontend-script=gs://bucket/scripts/my_logic.scala --pipeline-type=custom-analytics
  
  # System health check
  --operation=health-check
    """)
  }
}