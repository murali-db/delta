/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.icebergScanPlan

import org.apache.spark.sql.sources._

/**
 * Converts Spark Filter expressions to Iceberg expression JSON format.
 * MVP implementation supports: =, <, >, <=, >=, AND, OR operators.
 *
 * Iceberg expression format:
 * - EqualTo: {"type":"eq","term":"column","value":value}
 * - LessThan: {"type":"lt","term":"column","value":value}
 * - GreaterThan: {"type":"gt","term":"column","value":value}
 * - And: {"type":"and","left":expr1,"right":expr2}
 * - Or: {"type":"or","left":expr1,"right":expr2}
 */
object SparkToIcebergConverter {

  /**
   * Convert an array of Spark filters to a single Iceberg expression JSON string.
   * Multiple filters are combined with AND.
   *
   * @param filters Array of Spark filters
   * @return Optional JSON string representing the Iceberg expression,
   *         None if no convertible filters
   */
  def convert(filters: Array[Filter]): Option[String] = {
    if (filters.isEmpty) return None

    val convertedExprs = filters.flatMap(convertSingle)
    if (convertedExprs.isEmpty) return None

    // Combine multiple expressions with AND
    Some(combineWithAnd(convertedExprs))
  }

  /**
   * Convert a single Spark filter to Iceberg expression JSON.
   * Returns None if the filter cannot be converted.
   */
  private def convertSingle(filter: Filter): Option[String] = filter match {
    case EqualTo(attribute, value) =>
      Some(s"""{"type":"eq","term":"$attribute","value":${valueToJson(value)}}""")

    case LessThan(attribute, value) =>
      Some(s"""{"type":"lt","term":"$attribute","value":${valueToJson(value)}}""")

    case GreaterThan(attribute, value) =>
      Some(s"""{"type":"gt","term":"$attribute","value":${valueToJson(value)}}""")

    case LessThanOrEqual(attribute, value) =>
      Some(s"""{"type":"lteq","term":"$attribute","value":${valueToJson(value)}}""")

    case GreaterThanOrEqual(attribute, value) =>
      Some(s"""{"type":"gteq","term":"$attribute","value":${valueToJson(value)}}""")

    case And(left, right) =>
      for {
        leftExpr <- convertSingle(left)
        rightExpr <- convertSingle(right)
      } yield s"""{"type":"and","left":$leftExpr,"right":$rightExpr}"""

    case Or(left, right) =>
      for {
        leftExpr <- convertSingle(left)
        rightExpr <- convertSingle(right)
      } yield s"""{"type":"or","left":$leftExpr,"right":$rightExpr}"""

    case _ =>
      // Unsupported filter types return None
      None
  }

  /**
   * Convert a Spark value to JSON representation.
   */
  private def valueToJson(value: Any): String = value match {
    case s: String => s""""${escapeJson(s)}""""
    case n: Number => n.toString
    case b: Boolean => b.toString
    case null => "null"
    case _ => s""""${escapeJson(value.toString)}""""
  }

  /**
   * Escape special characters in JSON strings.
   */
  private def escapeJson(s: String): String = {
    s.replace("\\", "\\\\")
     .replace("\"", "\\\"")
     .replace("\n", "\\n")
     .replace("\r", "\\r")
     .replace("\t", "\\t")
  }

  /**
   * Combine multiple expressions with AND.
   * For a single expression, return it directly.
   * For multiple expressions, nest them with AND operations.
   */
  private def combineWithAnd(exprs: Array[String]): String = {
    exprs.length match {
      case 0 => throw new IllegalArgumentException("Cannot combine empty array")
      case 1 => exprs.head
      case _ =>
        // Combine first two, then recursively combine with rest
        val combined = s"""{"type":"and","left":${exprs.head},"right":${exprs(1)}}"""
        if (exprs.length == 2) {
          combined
        } else {
          combineWithAnd(Array(combined) ++ exprs.drop(2))
        }
    }
  }
}
