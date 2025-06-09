# init sbt
# echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
# curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
# sudo apt-get update
# sudo apt-get install sbt
# ============================================================================================================


git clone https://github.com/WasinW/dp_scala_sandbox.git
cd dp_scala_sandbox/

mkdir dp-backend-service
cd dp-backend-service

# Create directory structure
mkdir -p src/main/scala/com/dataplatform
mkdir -p src/main/scala/com/core
mkdir -p project


mv ../build.sbt build.sbt
mv ../build.properties project/build.properties
mv ../plugins.sbt project/plugins.sbt
mv ../MyDataprocJob.scala src/main/scala/com/dataplatform/MyDataprocJob.scala
mv ../BackendService.scala src/main/scala/com/dataplatform/BackendService.scala
mv ../SecretManager.scala src/main/scala/com/core/SecretManager.scala
mv ../DataConnector.scala src/main/scala/com/core/DataConnector.scala
mv ../DataTransformer.scala src/main/scala/com/core/DataTransformer.scala


# Clean previous builds
sbt clean
# Compile and create fat JAR
sbt assembly
# Check if JAR was created
ls -la target/scala-2.12/


# gsutil cp target/scala-2.12/dp-backend-service-1.0.jar gs://dataproc-staging-asia-southeast1-434326790648-gcyeg1qw/notebooks/lib_jars/
# gsutil ls gs://dataproc-staging-asia-southeast1-434326790648-gcyeg1qw/notebooks/lib_jars/
