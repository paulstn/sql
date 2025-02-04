/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import java.util.Locale;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.rest.model.PromQLQueryRequest;
import org.opensearch.sql.spark.rest.model.PromQLQueryResponse;
import org.opensearch.sql.spark.transport.model.PromQLActionRequest;
import org.opensearch.sql.spark.transport.model.PromQLActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportPromQLRequestAction
    extends HandledTransportAction<PromQLActionRequest, PromQLActionResponse> {

  private final AsyncQueryExecutorService asyncQueryExecutorService;
  private final OpenSearchSettings pluginSettings;

  public static final String NAME = "cluster:admin/opensearch/ql/promql";
  public static final ActionType<PromQLActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, PromQLActionResponse::new);

  @Inject
  public TransportPromQLRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      AsyncQueryExecutorServiceImpl jobManagementService,
      OpenSearchSettings pluginSettings) {
    super(NAME, transportService, actionFilters, PromQLActionRequest::new);
    this.asyncQueryExecutorService = jobManagementService;
    this.pluginSettings = pluginSettings;
  }

  @Override
  protected void doExecute(
      Task task, PromQLActionRequest request, ActionListener<PromQLActionResponse> listener) {
    try {
      if (!(Boolean) pluginSettings.getSettingValue(Settings.Key.ASYNC_QUERY_ENABLED)) {
        listener.onFailure(
            new IllegalAccessException(
                String.format(
                    Locale.ROOT,
                    "%s setting is " + "false",
                    Settings.Key.ASYNC_QUERY_ENABLED.getKeyValue())));
        return;
      }

      PromQLQueryRequest promQLQueryRequest = request.getPromQLQueryRequest();
      PromQLQueryResponse promQLQueryResponse =
          asyncQueryExecutorService.promQLQuery(
              promQLQueryRequest, new NullAsyncQueryRequestContext());
      String responseContent =
          new JsonResponseFormatter<PromQLQueryResponse>(JsonResponseFormatter.Style.PRETTY) {
            @Override
            protected Object buildJsonObject(PromQLQueryResponse response) {
              return response;
            }
          }.format(promQLQueryResponse);
      listener.onResponse(new PromQLActionResponse(responseContent));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
