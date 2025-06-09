package com.dataplatform.core

import com.google.cloud.secretmanager.v1.{SecretManagerServiceClient, SecretVersionName}
import scala.util.{Try, Success, Failure}

case class DatabaseCredentials(
  url: String,
  username: String,
  password: String,
  driver: String
)

object SecretManager {
  
  def getSecret(projectId: String, secretId: String, versionId: String = "latest"): String = {
    val client = SecretManagerServiceClient.create()
    
    try {
      val secretVersionName = SecretVersionName.of(projectId, secretId, versionId)
      val response = client.accessSecretVersion(secretVersionName)
      val secretValue = response.getPayload.getData.toStringUtf8
      
      client.close()
      secretValue
    } catch {
      case e: Exception =>
        client.close()
        throw new RuntimeException(s"Failed to get secret $secretId: ${e.getMessage}", e)
    }
  }
  
  def getDatabaseCredentials(projectId: String, secretId: String): DatabaseCredentials = {
    val secretJson = getSecret(projectId, secretId)
    parseJsonCredentials(secretJson)
  }
  
  private def parseJsonCredentials(jsonString: String): DatabaseCredentials = {
    // Simple JSON parsing - in production use circe or play-json
    val lines = jsonString.split("\n").map(_.trim).filter(_.nonEmpty)
    
    var url = ""
    var username = ""
    var password = ""
    var driver = ""
    
    lines.foreach { line =>
      if (line.contains("\"url\"")) {
        url = extractJsonValue(line)
      } else if (line.contains("\"username\"")) {
        username = extractJsonValue(line)
      } else if (line.contains("\"password\"")) {
        password = extractJsonValue(line)
      } else if (line.contains("\"driver\"")) {
        driver = extractJsonValue(line)
      }
    }
    
    DatabaseCredentials(url, username, password, driver)
  }
  
  private def extractJsonValue(line: String): String = {
    val parts = line.split(":")
    if (parts.length >= 2) {
      parts(1).trim.replaceAll("[\",]", "")
    } else ""
  }
}
