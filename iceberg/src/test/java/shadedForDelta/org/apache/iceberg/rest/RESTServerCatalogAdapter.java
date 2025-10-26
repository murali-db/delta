/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package shadedForDelta.org.apache.iceberg.rest;

import java.util.Map;
import java.util.function.Consumer;

import shadedForDelta.org.apache.iceberg.rest.responses.ErrorResponse;
import shadedForDelta.org.apache.iceberg.rest.RESTCatalogAdapter;

class RESTServerCatalogAdapter extends RESTCatalogAdapter {

  private final RESTCatalogServer.CatalogContext catalogContext;

  RESTServerCatalogAdapter(RESTCatalogServer.CatalogContext catalogContext) {
    super(catalogContext.catalog());
    this.catalogContext = catalogContext;
  }

  @Override
  public <T extends RESTResponse> T handleRequest(
      Route route, Map<String, String> vars, Object body, Class<T> responseType) {
    T restResponse = super.handleRequest(route, vars, body, responseType);

    /*
    if (restResponse instanceof LoadTableResponse) {
      if (PropertyUtil.propertyAsBoolean(
          catalogContext.configuration(), INCLUDE_CREDENTIALS, false)) {
        applyCredentials(
            catalogContext.configuration(), ((LoadTableResponse) restResponse).config());
      }
    }
    */
    return restResponse;
  }

  @Override
  protected <T extends RESTResponse> T execute(
          HTTPRequest request,
          Class<T> responseType,
          Consumer<ErrorResponse> errorHandler,
          Consumer<Map<String, String>> responseHeaders,
          ParserContext parserContext) {
    System.out.println("Executing request: " + request.method() + " " + request.path());
    return super.execute(
        request, responseType, errorHandler, responseHeaders, parserContext);
  }

  /*
  private void applyCredentials(
      Map<String, String> catalogConfig, Map<String, String> tableConfig) {
    if (catalogConfig.containsKey(S3FileIOProperties.ACCESS_KEY_ID)) {
      tableConfig.put(
          S3FileIOProperties.ACCESS_KEY_ID, catalogConfig.get(S3FileIOProperties.ACCESS_KEY_ID));
    }

    if (catalogConfig.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)) {
      tableConfig.put(
          S3FileIOProperties.SECRET_ACCESS_KEY,
          catalogConfig.get(S3FileIOProperties.SECRET_ACCESS_KEY));
    }

    if (catalogConfig.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
      tableConfig.put(
          S3FileIOProperties.SESSION_TOKEN, catalogConfig.get(S3FileIOProperties.SESSION_TOKEN));
    }

    if (catalogConfig.containsKey(GCPProperties.GCS_OAUTH2_TOKEN)) {
      tableConfig.put(
          GCPProperties.GCS_OAUTH2_TOKEN, catalogConfig.get(GCPProperties.GCS_OAUTH2_TOKEN));
    }

    catalogConfig.entrySet().stream()
        .filter(
            entry ->
                entry.getKey().startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX)
                    || entry.getKey().startsWith(AzureProperties.ADLS_CONNECTION_STRING_PREFIX))
        .forEach(entry -> tableConfig.put(entry.getKey(), entry.getValue()));
  } */
}
