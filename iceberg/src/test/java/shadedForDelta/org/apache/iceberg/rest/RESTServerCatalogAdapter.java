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
}
