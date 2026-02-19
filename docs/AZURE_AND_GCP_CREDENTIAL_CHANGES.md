# Azure and GCP (GCS) Credential Changes in This Fork

This document describes the **Azure** and **GCP (Google Cloud Storage)** credential handling changes implemented in this fork on top of the Delta Lake `branch-4.1` (master) branch. These changes support server-side planning with Unity Catalog (or other IRC-compatible catalogs) that vends temporary storage credentials for ABFS and GCS.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Credential Flow (End-to-End)](#2-credential-flow-end-to-end)
3. [Azure ADLS Gen2 Changes](#3-azure-adls-gen2-changes)
4. [GCP GCS Changes](#4-gcp-gcs-changes)
5. [Files Touched](#5-files-touched)
6. [Configuration Reference](#6-configuration-reference)
7. [Testing and Compatibility Notes](#7-testing-and-compatibility-notes)

---

## 1. Overview

- **Goal:** When the server returns a scan plan with **storage credentials**, the client injects those credentials into the Hadoop `Configuration` used to read Parquet files (e.g. on executors). This allows reading from cloud storage (S3, Azure ADLS Gen2, GCS) using short-lived tokens without embedding them in the table or relying on cluster-wide defaults.
- **Scope of this doc:** Only **Azure** and **GCP (GCS)**; S3 handling is unchanged from upstream.
- **Key components:**
  - **Parsing:** Map the JSON/config keys from the server into sealed credential types (`AzureCredentials`, `GcsCredentials`).
  - **Injection:** When building the partition reader factory, set the appropriate `fs.*` Hadoop properties so that ABFS and GCS connectors use the vended credentials.
  - **GCS only:** A custom Java `AccessTokenProvider` implementation that reads token and optional expiration from Hadoop conf and returns `AccessToken` for the GCS connector.

---

## 2. Credential Flow (End-to-End)

1. **Server** (e.g. Unity Catalog / IRC) returns a scan plan that includes a **storage-credentials** payload (e.g. in Iceberg REST, `storage-credentials[0].config`).
2. **Client** (e.g. `IcebergRESTCatalogPlanningClient`) parses the response and calls `ScanPlanStorageCredentials.fromConfig(config)` with the config map.
3. **`ScanPlanStorageCredentials`** (in `ServerSidePlanningClient.scala`) detects credential type by key patterns (S3, Azure, GCS) and builds the corresponding case class.
4. **`ServerSidePlannedFilePartitionReaderFactory`** (in `ServerSidePlannedTable.scala`) receives the credentials and, when building the Hadoop conf, matches on the credential type and sets the appropriate `fs.*` keys.
5. **Executors** use this conf (via `SerializableConfiguration`) when opening files; the ABFS or GCS connector reads the credentials from the conf (and for GCS, instantiates our custom provider and calls `getAccessToken()`).

---

## 3. Azure ADLS Gen2 Changes

### 3.1 Server-Side Config Keys (What the Server Sends)

The fork expects Azure credentials in the form used by Unity Catalog:

- **`adls.sas-token.<storageAccount>.dfs.core.windows.net`** — the SAS token value.
- **`adls.sas-token-expires-at-ms.<storageAccount>.dfs.core.windows.net`** — optional; expiration time in epoch milliseconds.

Example keys:

- `adls.sas-token.unitycatalogmetastore.dfs.core.windows.net`
- `adls.sas-token-expires-at-ms.unitycatalogmetastore.dfs.core.windows.net`

No fixed list of key names is required: any key **starting with** `adls.sas-token` is treated as part of the Azure credential set and passed through as-is.

### 3.2 Parsing (ServerSidePlanningClient.scala)

- **Detection:** `hasAzureKeys` is true if the config contains any key that **starts with** `"adls.sas-token"`.
- **Data structure:**  
  `AzureCredentials(accountName: String, containerName: String, credentialEntries: Map[String, String])`  
  - `credentialEntries`: all config entries whose key starts with `adls.sas-token` (both the token and the optional expires-at key).
  - `accountName`: derived from the SAS token key (the one that does not contain `sas-token-expires-at-ms`) by stripping the prefix and the `.dfs.core.windows.net` suffix (e.g. `unitycatalogmetastore`).
  - `containerName`: left empty (`""`) in this flow.

So the server can send the long, account-qualified key names and we still derive the account and pass all entries through.

### 3.3 Injection (ServerSidePlannedTable.scala)

For `AzureCredentials(accountName, _containerName, credentialEntries)` we set:

| Hadoop / ABFS key | Value |
|-------------------|--------|
| `fs.abfs.impl.disable.cache` | `true` |
| `fs.abfss.impl.disable.cache` | `true` |
| `fs.azure.account.auth.type.<account>.dfs.core.windows.net` | `SAS` |
| `fs.azure.sas.fixed.token.<account>.dfs.core.windows.net` | SAS token string |

Where `<account>` is the derived account name (e.g. `unitycatalogmetastore`), and the suffix is `.dfs.core.windows.net`. The SAS token value is taken from the credential entry whose key does **not** contain `sas-token-expires-at-ms` (the “token” key).

We **do not** set:

- `fs.azure.sas.token.provider.type` — the Hadoop Azure ABFS connector (e.g. 3.4.x) uses a default SAS token provider when auth type is SAS and a fixed token is set; explicitly setting the provider type was removed to avoid `ClassNotFoundException` or invalid-provider errors (e.g. with `FixedSASTokenProvider` class names that differ across Hadoop/Azure versions).
- Any key for the expiration timestamp — expiration is not passed into Hadoop conf in this fork; the connector uses the fixed token as provided.

This aligns with a minimal, stable ABFS SAS configuration that works with Hadoop Azure 3.4.x (and similar) on the classpath.

### 3.4 Logging

- Parsing: logs that Azure was matched, the number of keys, the derived account name, and that entries are used as-is.
- Injection: logs the exact keys and values set (including the SAS token value) for debugging; consider reducing or redacting in production if needed.

---

## 4. GCP GCS Changes

### 4.1 Server-Side Config Keys (What the Server Sends)

- **`gcs.oauth2.token`** (required) — OAuth2 access token string.
- **`gcs.oauth2.token-expires-at`** (optional) — expiration time in **epoch milliseconds** (e.g. `1771456336352`).

### 4.2 Parsing (ServerSidePlanningClient.scala)

- **Detection:** GCS is chosen if the config contains `gcs.oauth2.token`.
- **Data structure:**  
  `GcsCredentials(oauth2Token: String, expirationEpochMs: Option[Long] = None)`  
  - `expirationEpochMs`: parsed from `gcs.oauth2.token-expires-at` if present; otherwise `None`.

### 4.3 Custom GCS Access Token Provider (Java)

The GCS Hadoop connector (GoogleCloudDataproc hadoop-connectors / util-hadoop) supports an **ACCESS_TOKEN_PROVIDER** auth type: instead of a single fixed token, it loads a class that implements `com.google.cloud.hadoop.util.AccessTokenProvider` and calls `getAccessToken()` when it needs a token.

This fork adds a custom provider so that the token (and optional expiration) come from the same Hadoop `Configuration` we set at injection time.

**Class:**  
`org.apache.spark.sql.delta.serverSidePlanning.gcs.ConfBasedGcsAccessTokenProvider`  
**Location:**  
`spark/src/main/java/org/apache/spark/sql/delta/serverSidePlanning/gcs/ConfBasedGcsAccessTokenProvider.java`

**Behavior:**

- Implements `AccessTokenProvider`: `setConf`/`getConf`, `getAccessToken()`, `refresh()` (no-op).
- **Config keys read from Hadoop Configuration:**
  - **`fs.gs.auth.access.token`** (required) — OAuth2 token string.
  - **`fs.gs.auth.access.token.expiration.ms`** (optional) — expiration in epoch milliseconds. If missing or unparseable, a fallback of **1 hour from now** is used so the provider always returns a valid expiration.
- **Return value:**  
  `new AccessTokenProvider.AccessToken(token, Long.valueOf(expMs))`  
  The constructor used is **(String, Long)** for compatibility with the **older** util-hadoop API (e.g. hadoop2-2.2.x) that is often present on the runtime classpath when using Unity Catalog or the GCS connector. Newer util-hadoop versions use `(String, java.time.Instant)`; we compile against the older API so the same JAR works with the older connector.
- **Logging:**  
  On each `getAccessToken()` call, the provider logs the token string, expiration ms, whether expiration came from config, and a summary of the returned `AccessToken` (for debugging; consider reducing in production).

### 4.4 Injection (ServerSidePlannedTable.scala)

For `GcsCredentials(oauth2Token, expirationEpochMs)` we set:

| Hadoop / GCS key | Value |
|------------------|--------|
| `fs.gs.impl.disable.cache` | `true` |
| `fs.gs.auth.type` | `ACCESS_TOKEN_PROVIDER` |
| `fs.gs.auth.access.token.provider` | FQCN of `ConfBasedGcsAccessTokenProvider` |
| `fs.gs.auth.access.token.provider.impl` | Same FQCN (for connectors that expect `.impl`) |
| `fs.gs.auth.access.token` | `oauth2Token` |
| `fs.gs.auth.access.token.expiration.ms` | Set only when `expirationEpochMs.isDefined`; value is the epoch ms string. |

Both `fs.gs.auth.access.token.provider` and `fs.gs.auth.access.token.provider.impl` are set so that different GCS connector variants (e.g. Unity Catalog vs. stock Google connector) that look for either key will find our provider.

### 4.5 Build / Compile-Time Dependency (build.sbt)

The Java provider implements an interface from **com.google.cloud.bigdataoss:util-hadoop**, which is not part of the Delta repo. To compile the provider without packaging the connector into Delta’s JAR:

- **Dependency:**  
  `"com.google.cloud.bigdataoss" % "util-hadoop" % "hadoop2-2.2.26" % "provided"`  
  in the **spark** (sparkV1) module.
- **Why hadoop2-2.2.26:**  
  The **AccessToken** class in util-hadoop 3.x uses a constructor `(String, Instant)`, while the connector often present at runtime (e.g. with UC) uses an older util-hadoop with `(String, Long)`. Compiling against **hadoop2-2.2.26** uses the `(String, Long)` API, so the built Delta JAR is compatible with that runtime and avoids `NoSuchMethodError`.
- **Runtime:**  
  The cluster or `spark-sql`/`spark-submit` classpath must include the GCS connector (and hence `AccessTokenProvider`); Delta does not bundle it.

---

## 5. Files Touched

| Path | Summary of changes |
|------|--------------------|
| **spark/.../ServerSidePlanningClient.scala** | Azure: detect any key starting with `adls.sas-token`; build `AzureCredentials(accountName, containerName, credentialEntries)` from those entries; derive account from key. GCS: parse `gcs.oauth2.token` and optional `gcs.oauth2.token-expires-at` into `GcsCredentials(oauth2Token, expirationEpochMs)`. Added/extended logging for credential config and matching. |
| **spark/.../ServerSidePlannedTable.scala** | Azure: set `fs.abfs.impl.disable.cache`, `fs.abfss.impl.disable.cache`, `fs.azure.account.auth.type.<account>.dfs.core.windows.net`, `fs.azure.sas.fixed.token.<account>.dfs.core.windows.net`; no SAS token provider type. GCS: set `fs.gs.impl.disable.cache`, `fs.gs.auth.type=ACCESS_TOKEN_PROVIDER`, both provider keys, `fs.gs.auth.access.token`, and optionally `fs.gs.auth.access.token.expiration.ms`. Logging for keys (and for Azure, values) set. |
| **spark/.../gcs/ConfBasedGcsAccessTokenProvider.java** | **New.** Implements `AccessTokenProvider`; reads `fs.gs.auth.access.token` and `fs.gs.auth.access.token.expiration.ms`; returns `AccessToken(token, Long)`; fallback expiration 1 hour; debug logging. |
| **build.sbt** | In sparkV1, add `"com.google.cloud.bigdataoss" % "util-hadoop" % "hadoop2-2.2.26" % "provided"` for compiling the GCS provider. |
| **iceberg/.../IcebergRESTCatalogPlanningClient.scala** | Doc comment for `extractCredentials` / storage-credentials updated to show `gcs.oauth2.token` and optional `gcs.oauth2.token-expires-at` (epoch ms). Optional println of full response body for debugging. |
| **iceberg/.../IcebergRESTCatalogPlanningClientSuite.scala** | GCS credential tests: one case without expiration (`GcsCredentials(..., None)`), one with `gcs.oauth2.token-expires-at` and `GcsCredentials(..., Some(1771456336352L))`. (Azure test case in the suite may still use a legacy config shape; production Azure uses the adls.sas-token* keys above.) |

---

## 6. Configuration Reference

### 6.1 Azure: Server → Hadoop

| Server config key (example) | Hadoop conf we set |
|-----------------------------|--------------------|
| `adls.sas-token.<account>.dfs.core.windows.net` | Value used for `fs.azure.sas.fixed.token.<account>.dfs.core.windows.net` |
| (any key starting with `adls.sas-token`) | Used to derive account and to pick the token value; only the non–expires-at key’s value is set as fixed token |
| — | `fs.abfs.impl.disable.cache` = `true` |
| — | `fs.abfss.impl.disable.cache` = `true` |
| — | `fs.azure.account.auth.type.<account>.dfs.core.windows.net` = `SAS` |

### 6.2 GCS: Server → Hadoop

| Server config key | Hadoop conf we set |
|-------------------|--------------------|
| `gcs.oauth2.token` | `fs.gs.auth.access.token` |
| `gcs.oauth2.token-expires-at` (optional) | `fs.gs.auth.access.token.expiration.ms` (only when present) |
| — | `fs.gs.impl.disable.cache` = `true` |
| — | `fs.gs.auth.type` = `ACCESS_TOKEN_PROVIDER` |
| — | `fs.gs.auth.access.token.provider` = FQCN of our provider |
| — | `fs.gs.auth.access.token.provider.impl` = same FQCN |

---

## 7. Testing and Compatibility Notes

- **Scalastyle / format:** Some line-length and formatting adjustments were made (e.g. in `ServerSidePlannedTable.scala`) to satisfy the project’s scalastyle rules.
- **GCS at runtime:** Ensure the GCS Hadoop connector (and hence `AccessTokenProvider`) is on the classpath (e.g. via `--packages` or cluster libs). The util-hadoop version at runtime may be older (e.g. hadoop2-2.2.x); our provider is compiled for the `(String, Long)` `AccessToken` constructor to match that.
- **Azure at runtime:** Hadoop Azure (e.g. `hadoop-azure` 3.4.x) must be on the classpath. We do not set `fs.azure.sas.token.provider.type`; the connector’s default behavior with SAS auth type and fixed token is used.
- **Logging:** Several `println`/`System.out.println` calls were added for credential parsing and injection (and in the GCS provider). For production, consider switching to a proper logger and reducing or redacting sensitive values.
- **Tests:** `IcebergRESTCatalogPlanningClientSuite` includes GCS credential cases with and without expiration. Azure credential tests may still use an older config shape; the implementation supports the real UC-style `adls.sas-token*` keys described above.

---

*This document reflects the state of the fork as of the commits that introduced and refined the Azure and GCP credential handling on top of Delta Lake branch-4.1.*
