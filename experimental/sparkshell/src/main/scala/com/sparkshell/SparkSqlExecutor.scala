package com.sparkshell

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Try, Success, Failure}

class SparkSqlExecutor(spark: SparkSession) {

  def executeSql(sqlQuery: String): SqlResult = {
    Try {
      val df = spark.sql(sqlQuery)

      // Check if this is a DDL/DML command or a query
      if (df.schema.isEmpty) {
        // DDL/DML command (CREATE, INSERT, etc.)
        SqlResult(success = true, result = "Command executed successfully", error = None)
      } else {
        // Query that returns data
        val resultString = formatDataFrame(df)
        SqlResult(success = true, result = resultString, error = None)
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        SqlResult(success = false, result = "", error = Some(exception.getMessage))
    }
  }

  private def formatDataFrame(df: DataFrame): String = {
    val rows = df.collect()
    val schema = df.schema

    if (rows.isEmpty) {
      "Empty result set"
    } else {
      val header = schema.fields.map(_.name).mkString(" | ")
      val separator = "-" * header.length
      val data = rows.map { row =>
        row.toSeq.map(v => if (v == null) "null" else v.toString).mkString(" | ")
      }.mkString("\n")

      s"$header\n$separator\n$data\n\nTotal rows: ${rows.length}"
    }
  }
}

case class SqlResult(success: Boolean, result: String, error: Option[String])
