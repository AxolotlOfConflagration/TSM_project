name := "TSM_project"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "com.crealytics" %% "spark-excel" % "0.8.2",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion
)

val projectMainClass = "Main"

// set the main class for the main 'sbt run' task
mainClass in (Compile, run) := Some(projectMainClass)

mainClass in (Compile, packageBin) := Some(projectMainClass)