ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "1.0"

lazy val root = (project in file("."))
  .settings(
    name := "dp-backend-service",
    assembly / mainClass := Some("com.dataplatform.MyDataprocJob"),
    assembly / assemblyJarName := "dp-backend-service-1.0.jar",
    
    libraryDependencies ++= Seq(
      // Spark dependencies (provided by Dataproc)
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
      
      // Google Cloud dependencies
      "com.google.cloud" % "google-cloud-secretmanager" % "2.63.0",
      "com.google.cloud" % "google-cloud-storage" % "2.63.0",
      
      // Database drivers
      "com.oracle.database.jdbc" % "ojdbc8" % "21.9.0.0",
      
      // BigQuery connector (if needed later)
      "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.32.2"
    ),
    
    // Assembly merge strategy
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf" => MergeStrategy.concat
      case "module-info.class" => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    
    // Exclude Spark jars from assembly (provided by Dataproc)
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { jar =>
        jar.data.getName.contains("spark-core") ||
        jar.data.getName.contains("spark-sql") ||
        jar.data.getName.contains("hadoop")
      }
    }
  )
