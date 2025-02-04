/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.rest.model;

import lombok.Data;
import org.apache.commons.lang3.Validate;

@Data
public class PromQLQueryRequest {
  private String query;
  private String datasource;
  private Long starttime;
  private Long endtime;
  private String step;

  public PromQLQueryRequest(
      String query, String datasource, Long starttime, Long endtime, String step) {
    this.query = Validate.notNull(query, "Query can't be null");
    this.datasource = Validate.notNull(datasource, "Datasource can't be null");
    this.starttime = Validate.notNull(starttime, "Starttime can't be null");
    this.endtime = Validate.notNull(endtime, "Endtime can't be null");
    this.step = Validate.notNull(step, "Step can't be null");
  }
}
