import org.apache.spark.sql.SparkSession
import com.dataplatform.BackendService

/**
 * Demo Frontend Script for Data Platform POC
 * This script contains business logic and calls backend services
 */
object DemoFrontendPipeline {
  
  def main(args: Array[String]): Unit = {
    
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Demo-Frontend-Pipeline")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    
    try {
      println("=== Demo Frontend Pipeline Started ===")
      
      // Initialize backend service
      val backendService = new BackendService(spark)
      
      // Parse arguments
      val pipelineType = getArgValue(args, "--pipeline-type", "oracle-to-gcs")
      val projectId = getArgValue(args, "--project-id", sys.env.getOrElse("GOOGLE_CLOUD_PROJECT", "ntt-test-data-bq-looker"))
      val executionDate = getArgValue(args, "--execution-date", "2024-01-01")
      
      println(s"Pipeline Type: $pipelineType")
      println(s"Project ID: $projectId")
      println(s"Execution Date: $executionDate")
      
      // Execute different pipeline types
      pipelineType match {
        case "oracle-to-gcs" => runOracleToGCSPipeline(backendService, projectId, executionDate)
        case _ => 
          println(s"Unknown pipeline type: $pipelineType")
          showUsage()
      }
      
      println("=== Demo Frontend Pipeline Completed Successfully ===")
      
    } catch {
      case e: Exception =>
        println(s"Pipeline failed: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
  
  /**
   * Pipeline 1: Oracle to GCS (Simple Extract)
   */
  def runOracleToGCSPipeline(
    backendService: BackendService,
    projectId: String,
    executionDate: String
  ): Unit = {
    
    println("\n--- Running Oracle to GCS Pipeline ---")
    
    // Business Logic: Define what data to extract
    val businessQuery = s"""
    SELECT 
        e.name,
        e.department,
        COUNT(s.id) as total_sales,
        SUM(s.total_amount) as total_revenue
    FROM test_analytics.dbo.employees e
    LEFT JOIN test_analytics.dbo.sales s ON e.id = s.employee_id
    GROUP BY e.id, e.name, e.department
    ORDER BY total_revenue DESC
    """
    
    // Call Backend Service: Extract from Oracle
    println("1. Extracting data from Oracle...")
    val oracleData = backendService.extractFromOracle(
      projectId = projectId,
      // secretId = "oracle-db-credentials",
      secretId = "user_dev_db",
      query = businessQuery
    )
    
    println(s"Extracted ${oracleData.count()} records from Oracle")
    oracleData.show(5)
    
    // Call Backend Service: Standardize data
    println("2. Standardizing data...")
    // ====================================================================
    val standardizedData = backendService.standardizeData(oracleData)
    
    // Call Backend Service: Save to GCS
    println("3. Saving to GCS...")
    backendService.saveToGCS(
      df = standardizedData,
      outputPath = s"gs://dataproc-staging-asia-southeast1-434326790648-gcyeg1qw/output/oracle-extract/$executionDate",
      fileFormat = "parquet",
      mode = "overwrite"
    )
    
    println("✅ Oracle to GCS pipeline completed")
  }
  /**
   * Utility function to get argument values
   */
  private def getArgValue(args: Array[String], key: String, default: String): String = {
    args.find(_.startsWith(s"$key="))
      .map(_.split("=", 2)(1))
      .getOrElse(default)
  }
  
  /**
   * Show usage information
   */
  private def showUsage(): Unit = {
    println("""
Usage:
  spark-submit demo_scala_test.scala [options]

Options:
  --pipeline-type=TYPE     Pipeline type: oracle-to-gcs, gcs-to-gcs, oracle-transform
  --project-id=PROJECT     GCP Project ID
  --execution-date=DATE    Execution date (YYYY-MM-DD)

Examples:
  # Simple Oracle extraction
  --pipeline-type=oracle-to-gcs --execution-date=2024-01-01
  
  # File processing
  --pipeline-type=gcs-to-gcs
  
  # Complex transformation
  --pipeline-type=oracle-transform --execution-date=2024-01-01
    """)
  }
}
