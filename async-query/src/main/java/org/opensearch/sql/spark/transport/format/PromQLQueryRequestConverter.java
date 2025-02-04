/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.format;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import lombok.experimental.UtilityClass;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.rest.model.PromQLQueryRequest;

@UtilityClass
public class PromQLQueryRequestConverter {
  public static PromQLQueryRequest fromXContentParser(XContentParser parser) {
    String query = null;
    String datasource = null;
    Long starttime = null;
    Long endtime = null;
    String step = null;
    try {
      ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
      while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
        String fieldName = parser.currentName();
        parser.nextToken();
        if (fieldName.equals("query")) {
          query = parser.textOrNull();
        } else if (fieldName.equals("datasource")) {
          datasource = parser.textOrNull();
        } else if (fieldName.equals("starttime")) {
          starttime = parser.longValue();
        } else if (fieldName.equals("endtime")) {
          endtime = parser.longValue();
        } else if (fieldName.equals("step")) {
          step = parser.textOrNull();
        } else {
          throw new IllegalArgumentException("Unknown field: " + fieldName);
        }
      }
      return new PromQLQueryRequest(query, datasource, starttime, endtime, step);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Error while parsing the request body: %s", e.getMessage()));
    }
  }
}
