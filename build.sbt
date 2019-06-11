name := "FixedWidthFiletoParquet"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
// https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector
//libraryDependencies += "com.google.cloud.bigdataoss" % "gcs-connector" % "1.3.2-hadoop2"


