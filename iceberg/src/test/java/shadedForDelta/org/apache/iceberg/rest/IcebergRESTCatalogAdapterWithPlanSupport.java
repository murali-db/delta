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

package shadedForDelta.org.apache.iceberg.rest;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import shadedForDelta.org.apache.iceberg.FileScanTask;
import shadedForDelta.org.apache.iceberg.Table;
import shadedForDelta.org.apache.iceberg.TableScan;
import shadedForDelta.org.apache.iceberg.catalog.Catalog;
import shadedForDelta.org.apache.iceberg.catalog.TableIdentifier;
import shadedForDelta.org.apache.iceberg.io.CloseableIterable;
import shadedForDelta.org.apache.iceberg.rest.RESTCatalogAdapter;
import shadedForDelta.org.apache.iceberg.rest.requests.PlanTableScanRequest;
import shadedForDelta.org.apache.iceberg.rest.requests.PlanTableScanRequestParser;
import shadedForDelta.org.apache.iceberg.rest.responses.ErrorResponse;
import shadedForDelta.org.apache.iceberg.rest.PlanStatus;
import shadedForDelta.org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends RESTCatalogAdapter to add support for server-side scan planning via the /plan endpoint.
 * This adapter intercepts /plan requests and handles them by executing Iceberg table scans locally,
 * returning file scan tasks to the client. Other catalog operations are delegated to the parent
 * RESTCatalogAdapter implementation.
 */
class IcebergRESTCatalogAdapterWithPlanSupport extends RESTCatalogAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTCatalogAdapterWithPlanSupport.class);

  // Thread-local storage for passing credentials from adapter to servlet
  static final ThreadLocal<Map<String, String>> STORAGE_CREDENTIALS = new ThreadLocal<>();

  private final Catalog catalog;
  private final String ucBaseUrl;

  IcebergRESTCatalogAdapterWithPlanSupport(Catalog catalog, String ucBaseUrl) {
    super(catalog);
    this.catalog = catalog;
    this.ucBaseUrl = ucBaseUrl;
  }

  @Override
  protected <T extends RESTResponse> T execute(
          HTTPRequest request,
          Class<T> responseType,
          Consumer<ErrorResponse> errorHandler,
          Consumer<Map<String, String>> responseHeaders,
          ParserContext parserContext) {
    LOG.debug("Executing request: {} {}", request.method(), request.path());

    // Intercept /plan requests before they reach the base adapter
    if (isPlanTableScanRequest(request)) {
      try {
        PlanTableScanResponse response = handlePlanTableScan(request, parserContext);
        return (T) response;
      } catch (Exception e) {
        LOG.error("Error handling plan table scan: {}", e.getMessage(), e);
        ErrorResponse error = ErrorResponse.builder()
            .responseCode(500)
            .withType("InternalServerError")
            .withMessage("Failed to plan table scan: " + e.getMessage())
            .build();
        errorHandler.accept(error);
        return null;
      }
    }

    return super.execute(
        request, responseType, errorHandler, responseHeaders, parserContext);
  }

  private boolean isPlanTableScanRequest(HTTPRequest request) {
    return HTTPRequest.HTTPMethod.POST.equals(request.method()) &&
           request.path().endsWith("/plan");
  }

  private TableIdentifier extractTableIdentifier(String path) {
    // Path format: /v1/namespaces/{namespace}/tables/{table}/plan
    // or: /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan

    String[] parts = path.split("/");
    int namespacesIdx = -1;
    for (int i = 0; i < parts.length; i++) {
      if ("namespaces".equals(parts[i])) {
        namespacesIdx = i;
        break;
      }
    }

    if (namespacesIdx == -1 || namespacesIdx + 3 >= parts.length) {
      throw new IllegalArgumentException("Invalid path format: " + path);
    }

    String namespace = parts[namespacesIdx + 1];
    String tableName = parts[namespacesIdx + 3]; // skip "tables"

    return TableIdentifier.of(namespace, tableName);
  }

  private PlanTableScanRequest parsePlanRequest(HTTPRequest request) {
    // The request body should be a JSON string
    Object body = request.body();
    if (body == null) {
      throw new IllegalArgumentException("Request body is null");
    }
    String jsonBody = body.toString();
    return PlanTableScanRequestParser.fromJson(jsonBody);
  }

  /**
   * Fetches temporary storage credentials from Unity Catalog for a given table.
   * This is used when UC integration is enabled to get S3 credentials for data access.
   */
  private Map<String, String> fetchCredentialsFromUC(
      String catalog,
      String namespace,
      String table) throws IOException, InterruptedException {

    LOG.debug("Fetching credentials from UC for table: {}.{}.{}", catalog, namespace, table);

    // Step 1: Get table info from UC to extract table_id
    String tableFullName = catalog + "." + namespace + "." + table;
    String ucTableUrl = ucBaseUrl + "/api/2.1/unity-catalog/tables/" + tableFullName;

    HttpClient httpClient = HttpClient.newHttpClient();
    HttpRequest tableRequest = HttpRequest.newBuilder()
        .uri(URI.create(ucTableUrl))
        .GET()
        .build();

    HttpResponse<String> tableResponse = httpClient.send(
        tableRequest, HttpResponse.BodyHandlers.ofString());

    if (tableResponse.statusCode() != 200) {
      throw new IOException("Failed to get table info from UC: " +
          tableResponse.statusCode() + " - " + tableResponse.body());
    }

    // Parse table_id from response
    // Simple JSON parsing - assumes format: {"table_id":"...", ...}
    String tableResponseBody = tableResponse.body();
    String tableId = extractJsonField(tableResponseBody, "table_id");

    LOG.debug("Extracted table_id: {}", tableId);

    // Step 2: Get temporary credentials from UC
    String credsUrl = ucBaseUrl + "/api/2.1/unity-catalog/temporary-table-credentials";

    String credsRequestBody = String.format(
        "{\"table_id\":\"%s\",\"operation\":\"READ\"}", tableId);

    HttpRequest credRequest = HttpRequest.newBuilder()
        .uri(URI.create(credsUrl))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(credsRequestBody))
        .build();

    HttpResponse<String> credResponse = httpClient.send(
        credRequest, HttpResponse.BodyHandlers.ofString());

    if (credResponse.statusCode() != 200) {
      throw new IOException("Failed to get credentials from UC: " +
          credResponse.statusCode() + " - " + credResponse.body());
    }

    // Parse credentials from response
    // Expected format: {"aws_temp_credentials": {"access_key_id":"...", "secret_access_key":"...", "session_token":"..."}, "expiration_time":...}
    String credResponseBody = credResponse.body();

    Map<String, String> s3Config = new HashMap<>();
    s3Config.put("s3.access-key-id", extractNestedJsonField(credResponseBody, "aws_temp_credentials", "access_key_id"));
    s3Config.put("s3.secret-access-key", extractNestedJsonField(credResponseBody, "aws_temp_credentials", "secret_access_key"));

    // session_token might be optional
    try {
      String sessionToken = extractNestedJsonField(credResponseBody, "aws_temp_credentials", "session_token");
      if (sessionToken != null && !sessionToken.isEmpty()) {
        s3Config.put("s3.session-token", sessionToken);
      }
    } catch (Exception e) {
      LOG.debug("No session token in credentials (optional)");
    }

    // Add expiration time
    try {
      String expirationTime = extractJsonField(credResponseBody, "expiration_time");
      s3Config.put("s3.session-token-expires-at-ms", expirationTime);
    } catch (Exception e) {
      LOG.debug("No expiration time in credentials (optional)");
    }

    s3Config.put("client.region", "us-east-1");

    LOG.debug("Successfully fetched credentials from UC");
    return s3Config;
  }

  /**
   * Simple JSON field extractor. This is a basic implementation for testing.
   * In production, use a proper JSON library.
   */
  private String extractJsonField(String json, String fieldName) {
    String searchPattern = "\"" + fieldName + "\":\"";
    int startIndex = json.indexOf(searchPattern);
    if (startIndex == -1) {
      throw new IllegalArgumentException("Field " + fieldName + " not found in JSON: " + json);
    }
    startIndex += searchPattern.length();
    int endIndex = json.indexOf("\"", startIndex);
    if (endIndex == -1) {
      throw new IllegalArgumentException("Malformed JSON for field " + fieldName);
    }
    return json.substring(startIndex, endIndex);
  }

  /**
   * Extract nested JSON field (e.g., aws_temp_credentials.access_key_id)
   */
  private String extractNestedJsonField(String json, String parentField, String childField) {
    // Find the parent object
    String parentPattern = "\"" + parentField + "\":{";
    int parentStart = json.indexOf(parentPattern);
    if (parentStart == -1) {
      throw new IllegalArgumentException("Parent field " + parentField + " not found");
    }
    // Extract from parent onwards
    String fromParent = json.substring(parentStart + parentPattern.length());

    // Find the child field within parent
    return extractJsonField(fromParent, childField);
  }

  private PlanTableScanResponse handlePlanTableScan(
      HTTPRequest request,
      ParserContext parserContext) throws Exception {

    LOG.debug("Handling plan table scan request");

    // 1. Extract table identifier
    TableIdentifier tableIdent = extractTableIdentifier(request.path());
    LOG.debug("Table identifier: {}", tableIdent);

    // Check if UC integration is enabled
    boolean useUCIntegration = ucBaseUrl != null && !ucBaseUrl.isEmpty();
    Map<String, String> storageCredentials = null;

    if (useUCIntegration) {
      LOG.info("UC integration enabled - fetching credentials from UC");
      try {
        // Fetch credentials from Unity Catalog
        // Use "unity" as default catalog name (can be made configurable)
        String catalogName = "unity";
        storageCredentials = fetchCredentialsFromUC(
            catalogName,
            tableIdent.namespace().toString(),
            tableIdent.name());
        LOG.info("Successfully fetched credentials from UC for table {}.{}.{}",
            catalogName, tableIdent.namespace(), tableIdent.name());
      } catch (Exception e) {
        LOG.error("Failed to fetch credentials from UC: {}", e.getMessage(), e);
        throw new RuntimeException("Failed to fetch credentials from Unity Catalog", e);
      }
    }

    // 2. Parse request
    PlanTableScanRequest planRequest = parsePlanRequest(request);
    LOG.debug("Plan request parsed: snapshotId={}", planRequest.snapshotId());

    // 3. Load table from catalog
    Table table = catalog.loadTable(tableIdent);
    LOG.debug("Table loaded: {}", table);

    // 4. Create table scan
    TableScan tableScan = table.newScan();

    // 5. Apply snapshot if specified and valid
    if (planRequest.snapshotId() != null && planRequest.snapshotId() != 0) {
      tableScan = tableScan.useSnapshot(planRequest.snapshotId());
      LOG.debug("Using snapshot: {}", planRequest.snapshotId());
    } else {
      LOG.debug("Using current snapshot (snapshotId was null or 0)");
    }

    // 6. Validate that unsupported features are not requested
    if (planRequest.filter() != null) {
      throw new UnsupportedOperationException(
          "Filter pushdown is not supported in this test implementation");
    }
    if (planRequest.select() != null && !planRequest.select().isEmpty()) {
      throw new UnsupportedOperationException(
          "Column selection/projection is not supported in this test implementation");
    }
    if (planRequest.startSnapshotId() != null) {
      throw new UnsupportedOperationException(
          "Incremental scans are not supported in this test implementation");
    }
    if (planRequest.endSnapshotId() != null) {
      throw new UnsupportedOperationException(
          "Incremental scans are not supported in this test implementation");
    }
    if (planRequest.statsFields() != null && !planRequest.statsFields().isEmpty()) {
      throw new UnsupportedOperationException(
          "Column stats are not supported in this test implementation");
    }

    // 7. Execute scan planning
    List<FileScanTask> fileScanTasks = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
      tasks.forEach(task -> fileScanTasks.add(task));
    }
    LOG.debug("Planned {} file scan tasks", fileScanTasks.size());

    // 8. Get partition specs for serialization
    Map<Integer, shadedForDelta.org.apache.iceberg.PartitionSpec> specsById = table.specs();
    LOG.debug("Table has {} partition specs", specsById.size());

    // 9. Store credentials in thread-local for servlet to access
    if (useUCIntegration && storageCredentials != null) {
      LOG.info("Storing storage credentials in thread-local for servlet injection");
      STORAGE_CREDENTIALS.set(storageCredentials);
    } else {
      // Clear thread-local if no credentials
      STORAGE_CREDENTIALS.remove();
    }

    // 10. Build response (Pattern 1: COMPLETED with direct tasks)
    return PlanTableScanResponse.builder()
        .withPlanStatus(PlanStatus.COMPLETED)
        .withFileScanTasks(fileScanTasks)
        .withSpecsById(specsById)
        .build();
  }
}
