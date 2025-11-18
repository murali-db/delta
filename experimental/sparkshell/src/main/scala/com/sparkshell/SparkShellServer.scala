package com.sparkshell

import org.apache.spark.sql.SparkSession


class SparkShellServer(spark: SparkSession, port: Int) {
  private var restApi: RestApi = _

  def start(): Unit = {
    restApi = new RestApi(spark, port)
    restApi.start()

    sys.addShutdownHook {
      System.err.println("Shutting down REST server...")
      stop()
      spark.stop()
      System.err.println("Server shut down.")
    }
  }

  def stop(): Unit = {
    if (restApi != null) {
      restApi.stop()
    }
  }

  def blockUntilShutdown(): Unit = {
    // Keep the main thread alive
    try {
      Thread.currentThread().join()
    } catch {
      case _: InterruptedException =>
        println("Server interrupted, shutting down...")
    }
  }
}

object SparkShellServer {
  private val DEFAULT_PORT = 8080

  def main(args: Array[String]): Unit = {
    // Parse arguments: port [key1=value1 key2=value2 ...]
    val port = if (args.length > 0) args(0).toInt else DEFAULT_PORT
    val sparkConfigs = if (args.length > 1) {
      args.drop(1).map { arg =>
        val parts = arg.split("=", 2)
        if (parts.length == 2) Some((parts(0), parts(1))) else None
      }.flatten.toMap
    } else {
      Map.empty[String, String]
    }

    // Initialize Spark Session with Delta and Unity Catalog support
    val builder = SparkSession.builder()
      .appName("Spark SQL REST Server")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      // Delta Lake configurations
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // Unity Catalog configuration (catalog type only, URI and token must be provided via spark_configs)
      // .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
      // Cloud storage configurations (enable S3, Azure, GCS filesystems)
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.azure.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    
    // Apply custom Spark configurations
    val builderWithConfigs = sparkConfigs.foldLeft(builder) { case (b, (key, value)) =>
      println(s"Applying custom Spark config: $key = $value")
      b.config(key, value)
    }
    
    val spark = builderWithConfigs.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(s"Spark Session initialized: ${spark.version}")
    println("Delta Lake support enabled")
    
    // Check if Unity Catalog is configured
    if (sparkConfigs.contains("spark.sql.catalog.unity.uri") && 
        sparkConfigs.contains("spark.sql.catalog.unity.token")) {
      println(s"Unity Catalog enabled: ${sparkConfigs("spark.sql.catalog.unity.uri")}")
    } else {
      println("Unity Catalog available (provide spark.sql.catalog.unity.uri and spark.sql.catalog.unity.token to enable)")
    }

    // Eagerly initialize Spark internals to avoid lazy loading issues
    try {
      spark.sql("SELECT 1").collect()
      println("Spark internals pre-initialized successfully")
    } catch {
      case e: Exception =>
        println(s"Warning during Spark pre-initialization: ${e.getMessage}")
    }

    // Start REST API Server
    val server = new SparkShellServer(spark, port)
    server.start()
    server.blockUntilShutdown()
  }
}
