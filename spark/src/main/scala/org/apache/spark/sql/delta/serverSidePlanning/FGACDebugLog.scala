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

import java.io.{FileWriter, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Simple debug logger that writes FGAC debug output to ~/.sparkshell_cache/fgac_debug.log.
 * This is for debugging server-side planning issues.
 */
object FGACDebugLog {
  // Write to ~/.sparkshell_cache/fgac_debug.log (works in both local and notebook environments)
  private val LOG_FILE = {
    val homeDir = System.getProperty("user.home")
    val cacheDir = new java.io.File(homeDir, ".sparkshell_cache")
    cacheDir.mkdirs() // Ensure directory exists
    new java.io.File(cacheDir, "fgac_debug.log").getAbsolutePath
  }
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  // Clear log file on first use
  private var initialized = false

  private def ensureInitialized(): Unit = synchronized {
    if (!initialized) {
      val writer = new PrintWriter(new FileWriter(LOG_FILE, false))
      val ts = LocalDateTime.now().format(formatter)
      writer.write(s"=== FGAC Debug Log Started at $ts ===\n")
      writer.close()
      initialized = true
    }
  }

  def log(component: String, message: String): Unit = synchronized {
    ensureInitialized()
    val timestamp = LocalDateTime.now().format(formatter)
    val writer = new PrintWriter(new FileWriter(LOG_FILE, true))
    try {
      writer.write(s"[$timestamp] [$component] $message\n")
      writer.flush()
    } finally {
      writer.close()
    }
  }

  def logException(component: String, e: Throwable): Unit = synchronized {
    ensureInitialized()
    val timestamp = LocalDateTime.now().format(formatter)
    val writer = new PrintWriter(new FileWriter(LOG_FILE, true))
    try {
      val msg = s"[$timestamp] [$component] EXCEPTION: " +
        s"${e.getClass.getName}: ${e.getMessage}\n"
      writer.write(msg)
      e.printStackTrace(writer)
      writer.flush()
    } finally {
      writer.close()
    }
  }
}
