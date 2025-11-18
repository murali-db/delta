name := "SparkShell"

version := "0.1.0"

scalaVersion := "2.13.15"

// Main class for easy running
Compile / mainClass := Some("com.sparkshell.SparkShellServer")
assembly / mainClass := Some("com.sparkshell.SparkShellServer")
assembly / assemblyJarName := "sparkshell.jar"

// Assembly merge strategy
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _ => MergeStrategy.concat
      case "versions" :: _ => MergeStrategy.first
      case _ => MergeStrategy.discard
    }
  case "reference.conf" => MergeStrategy.concat
  case "module-info.class" => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "hadoop", xs @ _*) => MergeStrategy.first
  case PathList("com", "amazonaws", xs @ _*) => MergeStrategy.first
  case PathList("com", "google", xs @ _*) => MergeStrategy.first
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.endsWith(".properties") => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// JVM options for Java 9+ compatibility with Spark
fork := true
run / fork := true
run / connectInput := true
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false"
)

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-sql" % "4.0.0",
  
  // Delta Lake
  "io.delta" %% "delta-spark" % "4.0.0",
  
  // Unity Catalog
  "io.unitycatalog" % "unitycatalog-spark_2.13" % "0.3.0",
  
  // Cloud Storage Support (S3, ADLS, GCS)
  "org.apache.hadoop" % "hadoop-aws" % "3.4.0",
  "org.apache.hadoop" % "hadoop-azure" % "3.4.0",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.22",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262",
  
  // REST API
  "com.sparkjava" % "spark-core" % "2.9.4",
  "com.google.code.gson" % "gson" % "2.10.1",
  
  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)
