/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.model;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.sql.spark.rest.model.PromQLQueryRequest;

public class PromQLActionRequest extends ActionRequest {

  @Getter private PromQLQueryRequest promQLQueryRequest;

  /** Constructor of CreateJobActionRequest from StreamInput. */
  public PromQLActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  public PromQLActionRequest(PromQLQueryRequest promQLQueryRequest) {
    this.promQLQueryRequest = promQLQueryRequest;
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
