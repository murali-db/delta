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

package org.apache.spark.sql.delta.serverSidePlanning

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * Thread-safe registry for storing AWS credentials with automatic expiration.
 *
 * This registry is used by DynamicAwsCredentialsProvider to look up credentials
 * dynamically on every S3 API call, preventing credential caching issues.
 *
 * Key features:
 * - Thread-safe via ConcurrentHashMap
 * - Automatic TTL-based expiration
 * - Periodic cleanup of expired credentials
 * - Monitoring via stats() method
 */
object CredentialRegistry {
  private val credentials = new ConcurrentHashMap[String, CredentialEntry]()
  private val DEFAULT_TTL_MS = 60 * 60 * 1000L  // 1 hour

  // Thread-local storage for passing credential ID when setConf() isn't called
  private val threadLocalCredentialId = new ThreadLocal[String]()

  /**
   * Set the credential ID for the current thread.
   * This is used as a fallback when Hadoop doesn't call setConf().
   */
  def setThreadLocalCredentialId(credentialId: String): Unit = {
    threadLocalCredentialId.set(credentialId)
    // scalastyle:off println
    System.err.println(
      s"[CredentialRegistry] Set thread-local credential ID: $credentialId " +
      s"for thread: ${Thread.currentThread().getName}")
    // scalastyle:on println
  }

  /**
   * Get the credential ID for the current thread.
   */
  def getThreadLocalCredentialId(): Option[String] = {
    Option(threadLocalCredentialId.get())
  }

  /**
   * Clear the thread-local credential ID.
   */
  def clearThreadLocalCredentialId(): Unit = {
    threadLocalCredentialId.remove()
  }

  /**
   * Register credentials and get a unique credential ID.
   *
   * @param accessKeyId AWS access key ID
   * @param secretAccessKey AWS secret access key
   * @param sessionToken AWS session token
   * @param ttlMs Time-to-live in milliseconds (default: 1 hour)
   * @return Unique credential ID (UUID)
   */
  def register(
      accessKeyId: String,
      secretAccessKey: String,
      sessionToken: String,
      ttlMs: Long = DEFAULT_TTL_MS): String = {
    val credentialId = java.util.UUID.randomUUID().toString
    val entry = CredentialEntry(
      accessKeyId = accessKeyId,
      secretAccessKey = secretAccessKey,
      sessionToken = sessionToken,
      expirationTime = System.currentTimeMillis() + ttlMs
    )
    credentials.put(credentialId, entry)

    // scalastyle:off println
    System.err.println(s"[CredentialRegistry] Registered credentials: " +
      s"id=$credentialId, accessKey=${accessKeyId.take(8)}..., " +
      s"expiresIn=${ttlMs / 1000}s")
    // scalastyle:on println

    credentialId
  }

  /**
   * Retrieve credentials by ID.
   *
   * @param credentialId Unique credential ID
   * @return Some(CredentialEntry) if found and not expired, None otherwise
   */
  def get(credentialId: String): Option[CredentialEntry] = {
    Option(credentials.get(credentialId)).flatMap { entry =>
      if (entry.isExpired) {
        credentials.remove(credentialId)
        // scalastyle:off println
        System.err.println(
          s"[CredentialRegistry] Credential expired and removed: id=$credentialId")
        // scalastyle:on println
        None
      } else {
        Some(entry)
      }
    }
  }

  /**
   * Remove all expired credentials from the registry.
   *
   * @return Number of credentials removed
   */
  def cleanupExpired(): Int = {
    val now = System.currentTimeMillis()
    val expiredKeys = credentials.asScala.filter { case (_, entry) =>
      entry.expirationTime < now
    }.keys.toList

    expiredKeys.foreach(credentials.remove)

    if (expiredKeys.nonEmpty) {
      // scalastyle:off println
      System.err.println(
        s"[CredentialRegistry] Cleaned up ${expiredKeys.size} expired credentials")
      // scalastyle:on println
    }

    expiredKeys.size
  }

  /**
   * Get statistics about the credential registry.
   *
   * @return Registry statistics
   */
  def stats(): RegistryStats = {
    val now = System.currentTimeMillis()
    val allEntries = credentials.asScala.values.toList
    val activeEntries = allEntries.count(_.expirationTime >= now)
    val expiredEntries = allEntries.count(_.expirationTime < now)

    RegistryStats(
      totalEntries = allEntries.size,
      activeEntries = activeEntries,
      expiredEntries = expiredEntries
    )
  }

  /**
   * Clear all credentials from the registry.
   * Used primarily for testing.
   */
  def clear(): Unit = {
    val size = credentials.size()
    credentials.clear()
    // scalastyle:off println
    System.err.println(s"[CredentialRegistry] Cleared all credentials (removed $size entries)")
    // scalastyle:on println
  }
}

/**
 * Entry storing AWS credentials with expiration time.
 *
 * @param accessKeyId AWS access key ID
 * @param secretAccessKey AWS secret access key
 * @param sessionToken AWS session token
 * @param expirationTime Unix timestamp (milliseconds) when credentials expire
 */
case class CredentialEntry(
    accessKeyId: String,
    secretAccessKey: String,
    sessionToken: String,
    expirationTime: Long) {

  /**
   * Check if credentials have expired.
   *
   * @return true if expired, false otherwise
   */
  def isExpired: Boolean = System.currentTimeMillis() > expirationTime
}

/**
 * Statistics about the credential registry.
 *
 * @param totalEntries Total number of credential entries
 * @param activeEntries Number of active (non-expired) entries
 * @param expiredEntries Number of expired entries
 */
case class RegistryStats(
    totalEntries: Int,
    activeEntries: Int,
    expiredEntries: Int)
