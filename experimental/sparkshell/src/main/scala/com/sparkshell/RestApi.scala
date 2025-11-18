package com.sparkshell

import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import spark.{Request, Response, Spark}

class RestApi(sparkSession: SparkSession, port: Int) {

  private val gson = new Gson()
  private val executor = new SparkSqlExecutor(sparkSession)

  def start(): Unit = {
    // Set port
    Spark.port(port)

    // Enable CORS
    Spark.before((request, response) => {
      response.header("Access-Control-Allow-Origin", "*")
      response.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
      response.header("Access-Control-Allow-Headers", "Content-Type")
    })

    // Health check endpoint
    Spark.get("/health", (req: Request, res: Response) => {
      res.`type`("application/json")
      """{"status":"ok","message":"SparkApp server is running"}"""
    })

    // Execute SQL endpoint
    Spark.post("/sql", (req: Request, res: Response) => {
      res.`type`("application/json")

      try {
        val requestBody = req.body()
        val sqlRequest = gson.fromJson(requestBody, classOf[SqlRequestJson])

        if (sqlRequest.sql == null || sqlRequest.sql.trim.isEmpty) {
          res.status(400)
          gson.toJson(SqlResponseJson(
            success = false,
            result = null,
            error = "SQL query is required"
          ))
        } else {

          println(s"Executing SQL: ${sqlRequest.sql}")
          val result = executor.executeSql(sqlRequest.sql)

          val response = SqlResponseJson(
            success = result.success,
            result = if (result.success) result.result else null,
            error = result.error.orNull
          )

          if (!result.success) {
            res.status(400)
          }

          gson.toJson(response)
        }
      } catch {
        case e: Exception =>
          res.status(500)
          gson.toJson(SqlResponseJson(
            success = false,
            result = null,
            error = s"Server error: ${e.getMessage}"
          ))
      }
    })

    // Info endpoint
    Spark.get("/info", (req: Request, res: Response) => {
      res.`type`("application/json")
      val info = ServerInfo(
        sparkVersion = sparkSession.version,
        port = port.toString,
        endpoints = EndpointsInfo(
          health = "GET /health",
          execute = "POST /sql",
          info = "GET /info"
        )
      )
      gson.toJson(info)
    })

    // Wait for initialization
    Spark.awaitInitialization()
    println(s"REST API server started on port $port")
    println(s"Available endpoints:")
    println(s"  GET  http://localhost:$port/health - Health check")
    println(s"  GET  http://localhost:$port/info - Server info")
    println(s"  POST http://localhost:$port/sql - Execute SQL")
  }

  def stop(): Unit = {
    Spark.stop()
  }
}

// JSON request/response classes
case class SqlRequestJson(sql: String)

case class SqlResponseJson(
  success: Boolean,
  result: String,
  error: String
)

case class EndpointsInfo(
  health: String,
  execute: String,
  info: String
)

case class ServerInfo(
  sparkVersion: String,
  port: String,
  endpoints: EndpointsInfo
)
