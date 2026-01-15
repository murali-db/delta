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

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicSessionCredentials}
import org.apache.hadoop.conf.{Configuration, Configurable}

/**
 * Dynamic AWS credentials provider that never caches credentials.
 *
 * This provider implements the AWS SDK v1 AWSCredentialsProvider interface
 * and Hadoop's Configurable interface. It looks up credentials from the
 * CredentialRegistry on EVERY call to getCredentials(), ensuring that
 * fresh credentials are always used.
 *
 * Configuration:
 * - fs.s3a.credential.id: Required. UUID of credentials in the registry.
 * - fs.s3a.aws.credentials.provider: Set to this class name to use it.
 *
 * Key features:
 * - No internal credential caching
 * - Looks up credentials from registry on every S3 API call
 * - Fail-fast with clear error messages
 * - Thread-safe (registry is thread-safe)
 *
 * Usage:
 * {{{
 * conf.set("fs.s3a.credential.id", credentialId)
 * conf.set("fs.s3a.aws.credentials.provider",
 *   "org.apache.spark.sql.delta.serverSidePlanning.DynamicAwsCredentialsProvider")
 * }}}
 */
class DynamicAwsCredentialsProvider
    extends AWSCredentialsProvider with Configurable {

  @transient private var credentialId: String = _
  @transient private var conf: Configuration = _

  /**
   * Called by Hadoop to inject the Configuration.
   *
   * This method is called once during FileSystem initialization.
   * It reads the credential ID from the configuration and validates it.
   *
   * @param conf Hadoop configuration
   * @throws IllegalStateException if fs.s3a.credential.id is not set
   */
  override def setConf(conf: Configuration): Unit = {
    // scalastyle:off println
    val threadName = Thread.currentThread().getName
    System.err.println(
      s"[DynamicAwsCredentialsProvider] setConf() called on thread: $threadName")
    // scalastyle:on println

    this.conf = conf
    credentialId = conf.get("fs.s3a.credential.id")

    // scalastyle:off println
    System.err.println(
      s"[DynamicAwsCredentialsProvider] Read credential ID from conf: $credentialId")
    // scalastyle:on println

    if (credentialId == null || credentialId.isEmpty) {
      throw new IllegalStateException(
        "fs.s3a.credential.id must be set. " +
        "This indicates ServerSidePlannedTable did not configure credentials properly. " +
        "Check that CredentialRegistry.register() was called and the credential ID " +
        "was set in the configuration."
      )
    }

    // scalastyle:off println
    System.err.println(
      s"[DynamicAwsCredentialsProvider] Initialized with credential ID: $credentialId")
    // scalastyle:on println
  }

  /**
   * Get the Hadoop configuration.
   *
   * @return Hadoop configuration
   */
  override def getConf: Configuration = conf

  /**
   * Get AWS credentials from the registry.
   *
   * This method is called by the AWS SDK on EVERY S3 API call.
   * It performs a fresh lookup from the CredentialRegistry, ensuring
   * that the latest credentials are always used.
   *
   * @return AWS credentials (BasicSessionCredentials with temporary session token)
   * @throws IllegalStateException if credentials not found in registry
   */
  override def getCredentials(): AWSCredentials = {
    // Try multiple sources for credential ID:
    // 1. Instance variable (set by setConf())
    // 2. Thread-local storage (fallback when setConf() not called)
    // 3. Configuration object (if available)
    val actualCredentialId = if (credentialId != null && credentialId.nonEmpty) {
      credentialId
    } else {
      // setConf() wasn't called, try thread-local
      CredentialRegistry.getThreadLocalCredentialId().getOrElse {
        // Last resort: try reading from conf if available
        if (conf != null) {
          conf.get("fs.s3a.credential.id")
        } else {
          null
        }
      }
    }

    if (actualCredentialId == null || actualCredentialId.isEmpty) {
      val threadId = Thread.currentThread().getName
      throw new IllegalStateException(
        s"Credential ID is null or empty on thread: $threadId. " +
        "This indicates setConf() was not called and no thread-local credential ID was set. " +
        s"Registry stats: ${CredentialRegistry.stats()}"
      )
    }

    val entry = CredentialRegistry.get(actualCredentialId).getOrElse {
      throw new IllegalStateException(
        s"Credentials not found for ID: $actualCredentialId. " +
        s"Possible causes: " +
        s"1) Credentials expired (check TTL), " +
        s"2) Registry was cleared, " +
        s"3) Credentials were never registered. " +
        s"Registry stats: ${CredentialRegistry.stats()}"
      )
    }

    // Log credential retrieval (only log first few characters of access key for security)
    // scalastyle:off println
    System.err.println(s"[DynamicAwsCredentialsProvider] Retrieved credentials: " +
      s"id=$actualCredentialId, accessKey=${entry.accessKeyId.take(8)}...")
    // scalastyle:on println

    new BasicSessionCredentials(
      entry.accessKeyId,
      entry.secretAccessKey,
      entry.sessionToken
    )
  }

  /**
   * Refresh credentials.
   *
   * This is a no-op because credentials are always fresh from the registry.
   * The AWS SDK may call this method, but we don't need to do anything
   * since getCredentials() always performs a fresh lookup.
   */
  override def refresh(): Unit = {
    // No-op: credentials are always fresh from the registry
    // scalastyle:off println
    System.err.println(s"[DynamicAwsCredentialsProvider] refresh() called (no-op)")
    // scalastyle:on println
  }
}
