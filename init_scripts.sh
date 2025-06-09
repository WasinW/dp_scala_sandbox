

mkdir dp-backend-service
cd dp-backend-service

# Create directory structure
mkdir -p src/main/scala/com/dataplatform/core
mkdir -p project


mv ../build.sbt dp-backend-service/build.sbt
mv ../build.properties dp-backend-service/project/build.properties
mv ../plugins.sbt dp-backend-service/project/plugins.sbt
mv ../MyDataprocJob.scala dp-backend-service/src/main/scala/com/dataplatform/MyDataprocJob.scala
mv ../BackendService.scala dp-backend-service/src/main/scala/com/dataplatform/BackendService.scala
mv ../SecretManager.scala dp-backend-service/src/main/scala/com/core/SecretManager.scala
mv ../DataConnector.scala dp-backend-service/src/main/scala/com/core/DataConnector.scala
mv ../DataTransformer.scala dp-backend-service/src/main/scala/com/core/DataTransformer.scala
