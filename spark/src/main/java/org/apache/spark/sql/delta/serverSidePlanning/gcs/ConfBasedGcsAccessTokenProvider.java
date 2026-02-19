/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.serverSidePlanning.gcs;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;

import java.time.Instant;

/**
 * GCS AccessTokenProvider that reads token and optional expiration from Hadoop Configuration.
 * Used when server-side planning supplies temporary GCS OAuth2 credentials.
 * Config keys: fs.gs.auth.access.token (required), fs.gs.auth.access.token.expiration.ms (optional; epoch ms).
 * When expiration is missing, uses a fallback of 1 hour from now.
 */
public class ConfBasedGcsAccessTokenProvider implements AccessTokenProvider {

  private static final String CONFIG_TOKEN = "fs.gs.auth.access.token";
  private static final String CONFIG_EXPIRATION_MS = "fs.gs.auth.access.token.expiration.ms";
  private static final long FALLBACK_EXPIRATION_MS = 3600_000L; // 1 hour

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public AccessTokenProvider.AccessToken getAccessToken() {
    String token = conf.get(CONFIG_TOKEN);
    if (token == null || token.isEmpty()) {
      throw new IllegalStateException("Missing required config: " + CONFIG_TOKEN);
    }
    long expMs;
    String expStr = conf.get(CONFIG_EXPIRATION_MS);
    if (expStr != null && !expStr.isEmpty()) {
      try {
        expMs = Long.parseLong(expStr.trim());
      } catch (NumberFormatException e) {
        expMs = System.currentTimeMillis() + FALLBACK_EXPIRATION_MS;
      }
    } else {
      expMs = System.currentTimeMillis() + FALLBACK_EXPIRATION_MS;
    }
    System.out.println("[ConfBasedGcsAccessTokenProvider] getAccessToken() called: using token " +
        "(expirationMs=" + expMs + ", fromConfig=" + (expStr != null && !expStr.isEmpty()) + ")");
    return new AccessTokenProvider.AccessToken(token, Instant.ofEpochMilli(expMs));
  }

  @Override
  public void refresh() {
    // No-op: token is pre-set from the server for short-lived jobs.
  }
}
