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
import org.scalatest.funsuite.AnyFunSuite

class SparkToIcebergConverterSuite extends AnyFunSuite {

  test("convert - EqualTo with integer value") {
    val filter = EqualTo("id", 5)
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert EqualTo")
    assert(result.get == """{"type":"eq","term":"id","value":5}""",
      s"Unexpected JSON: ${result.get}")
  }

  test("convert - EqualTo with string value") {
    val filter = EqualTo("name", "Alice")
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert EqualTo with string")
    assert(result.get == """{"type":"eq","term":"name","value":"Alice"}""",
      s"Unexpected JSON: ${result.get}")
  }

  test("convert - LessThan") {
    val filter = LessThan("age", 30)
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert LessThan")
    assert(result.get == """{"type":"lt","term":"age","value":30}""",
      s"Unexpected JSON: ${result.get}")
  }

  test("convert - GreaterThan") {
    val filter = GreaterThan("score", 100)
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert GreaterThan")
    assert(result.get == """{"type":"gt","term":"score","value":100}""",
      s"Unexpected JSON: ${result.get}")
  }

  test("convert - LessThanOrEqual") {
    val filter = LessThanOrEqual("price", 50.5)
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert LessThanOrEqual")
    assert(result.get == """{"type":"lteq","term":"price","value":50.5}""",
      s"Unexpected JSON: ${result.get}")
  }

  test("convert - GreaterThanOrEqual") {
    val filter = GreaterThanOrEqual("rating", 4.0)
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert GreaterThanOrEqual")
    assert(result.get == """{"type":"gteq","term":"rating","value":4.0}""",
      s"Unexpected JSON: ${result.get}")
  }

  test("convert - And expression") {
    val filter = And(
      EqualTo("id", 5),
      LessThan("age", 30)
    )
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert And")
    val expected = """{"type":"and","left":{"type":"eq","term":"id","value":5},"right":{"type":"lt","term":"age","value":30}}"""
    assert(result.get == expected, s"Unexpected JSON: ${result.get}")
  }

  test("convert - Or expression") {
    val filter = Or(
      EqualTo("status", "active"),
      EqualTo("status", "pending")
    )
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert Or")
    val expected = """{"type":"or","left":{"type":"eq","term":"status","value":"active"},"right":{"type":"eq","term":"status","value":"pending"}}"""
    assert(result.get == expected, s"Unexpected JSON: ${result.get}")
  }

  test("convert - multiple filters combined with AND") {
    val filters: Array[Filter] = Array[Filter](
      EqualTo("id", 5),
      GreaterThan("age", 18),
      LessThan("score", 100)
    )
    val result = SparkToIcebergConverter.convert(filters)

    assert(result.isDefined, "Should convert multiple filters")
    // Should combine all three with AND
    assert(result.get.contains(""""type":"and""""), "Should contain AND")
    assert(result.get.contains(""""type":"eq""""), "Should contain EqualTo")
    assert(result.get.contains(""""type":"gt""""), "Should contain GreaterThan")
    assert(result.get.contains(""""type":"lt""""), "Should contain LessThan")
  }

  test("convert - empty array returns None") {
    val result = SparkToIcebergConverter.convert(Array.empty)
    assert(result.isEmpty, "Empty array should return None")
  }

  test("convert - unsupported filter returns None") {
    val filter = StringStartsWith("name", "A")
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isEmpty, "Unsupported filter should return None")
  }

  test("convert - mixed supported and unsupported filters") {
    val filters: Array[Filter] = Array[Filter](
      EqualTo("id", 5),           // Supported
      StringStartsWith("name", "A"), // Unsupported
      GreaterThan("age", 18)      // Supported
    )
    val result = SparkToIcebergConverter.convert(filters)

    assert(result.isDefined, "Should convert supported filters")
    // Should only include the two supported filters
    assert(result.get.contains(""""term":"id""""), "Should contain id filter")
    assert(result.get.contains(""""term":"age""""), "Should contain age filter")
    assert(!result.get.contains("name"), "Should NOT contain unsupported name filter")
  }

  test("convert - string value with special characters") {
    // Test string escaping with quotes and other special characters
    val filter = EqualTo("description", "Hello \"World\" with\nnew lines")
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert string with special chars")
    // The escapeJson function should produce valid JSON
    val json = result.get
    assert(json.contains("""\"World\""""), s"Should escape quotes. JSON: $json")
    // Verify the JSON is well-formed (contains type, term, value fields)
    assert(json.contains(""""type":"eq""""), "Should contain type field")
    assert(json.contains(""""term":"description""""), "Should contain term field")
    assert(json.contains(""""value":"""), "Should contain value field")
  }

  test("convert - boolean value") {
    val filter = EqualTo("active", true)
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert boolean")
    assert(result.get.contains("true"), "Should contain boolean value")
  }

  test("convert - nested And/Or expressions") {
    val filter = And(
      Or(EqualTo("status", "active"), EqualTo("status", "pending")),
      GreaterThan("age", 18)
    )
    val result = SparkToIcebergConverter.convert(Array(filter))

    assert(result.isDefined, "Should convert nested expressions")
    assert(result.get.contains(""""type":"and""""), "Should contain AND")
    assert(result.get.contains(""""type":"or""""), "Should contain OR")
  }
}
