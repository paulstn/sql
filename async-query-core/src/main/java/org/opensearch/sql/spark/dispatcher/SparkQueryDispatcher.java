/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import okhttp3.OkHttpClient;
import org.json.JSONObject;
import org.opensearch.sql.common.interceptors.URIValidatorInterceptor;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.QueryState;
import org.opensearch.sql.spark.client.PrometheusClient;
import org.opensearch.sql.spark.client.PrometheusClientImpl;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.utils.SQLQueryUtils;
import org.opensearch.sql.spark.validator.PPLQueryValidator;
import org.opensearch.sql.spark.validator.SQLQueryValidator;

/** This class takes care of understanding query and dispatching job query to emr serverless. */
@AllArgsConstructor
public class SparkQueryDispatcher {

  public static final String INDEX_TAG_KEY = "index";
  public static final String DATASOURCE_TAG_KEY = "datasource";
  public static final String CLUSTER_NAME_TAG_KEY = "domain_ident";
  public static final String JOB_TYPE_TAG_KEY = "type";

  private final DataSourceService dataSourceService;
  private final SessionManager sessionManager;
  private final QueryHandlerFactory queryHandlerFactory;
  private final QueryIdProvider queryIdProvider;
  private final SQLQueryValidator sqlQueryValidator;
  private final PPLQueryValidator pplQueryValidator;

  // private final PrometheusClient prometheusClient;

  public DispatchQueryResponse dispatch(
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            dispatchQueryRequest.getDatasource(), asyncQueryRequestContext);

    System.out.println("data source name: " + dataSourceMetadata.getName());
    System.out.println(
        "prom uri: "
            + dataSourceMetadata.getProperties().getOrDefault("prometheus.uri", "no prom uri"));
    if (LangType.PROMQL.equals(dispatchQueryRequest.getLangType())
        && dataSourceMetadata.getConnector() == DataSourceType.PROMETHEUS) {

      try {
        PrometheusClient prometheus =
            AccessController.doPrivileged(
                (PrivilegedExceptionAction<PrometheusClientImpl>)
                    () -> {
                      try {
                        return new PrometheusClientImpl(
                            getHttpClient(),
                            new URI(
                                dataSourceMetadata
                                    .getProperties()
                                    .getOrDefault("prometheus.uri", "no prom uri")));
                      } catch (URISyntaxException e) {
                        throw new IllegalArgumentException(
                            String.format(
                                "Invalid URI in prometheus properties: %s", e.getMessage()));
                      }
                    });

        // new PrometheusClientImpl(
        //     getHttpClient(),
        //     new URI(
        //         dataSourceMetadata
        //             .getProperties()
        //             .getOrDefault("prometheus.uri", "no prom uri")));

        System.out.println("created prom client impl");
        JSONObject res =
            AccessController.doPrivileged(
                (PrivilegedExceptionAction<JSONObject>)
                    () -> {
                      try {
                        return prometheus.queryRange(
                            dispatchQueryRequest.getQuery(), 1737964800L, 1738137600L, "1h");
                      } catch (IOException e) {
                        e.printStackTrace();
                        return new JSONObject();
                      }
                    });

        // System.out.println("res: " + res);

        return DispatchQueryResponse.builder()
            .queryId(res.toString())
            .jobId("yourJobId")
            .resultIndex("yourResultIndex")
            .sessionId("yourSessionId")
            .datasourceName("yourDatasourceName")
            .jobType(JobType.INTERACTIVE)
            .indexName("yourIndexName")
            .status(QueryState.SUCCESS)
            .build();

      } catch (PrivilegedActionException e) {
        e.printStackTrace();
        return DispatchQueryResponse.builder()
            .queryId("yourQueryId")
            .jobId("yourJobId")
            .resultIndex("yourResultIndex")
            .sessionId("yourSessionId")
            .datasourceName("yourDatasourceName")
            .jobType(JobType.INTERACTIVE)
            .indexName("yourIndexName")
            .status(QueryState.FAILED)
            .build();
      }

      // DataSource prometheus = this.dataSourceService.getDataSource(dataSourceMetadata.getName());

      // String prometheusUri =
      //     dataSourceMetadata.getProperties().getOrDefault("prometheus.uri", "no prom uri");
      // // make a call to prometheus

      // Collection<FunctionResolver> functionList = (prometheus.getStorageEngine()).getFunctions();
      // Optional<FunctionResolver> queryRangeResolver =
      //     functionList.stream()
      //         .filter(function -> function.getFunctionName().toString().equals("query_range"))
      //         .findFirst();

      // if (queryRangeResolver.isPresent()) {
      //   Pair<FunctionSignature, FunctionBuilder> queryRangeResolved =
      //       queryRangeResolver.get().resolve(null);
      //   List<Expression> arguments = new ArrayList<Expression>();
      //   arguments.add(
      //       new NamedArgumentExpression(
      //           "query",
      //           new LiteralExpression(
      //               new ExprStringValue("prometheus_tsdb_head_series{job=\"prometheus\"}"))));
      //   arguments.add(
      //       new NamedArgumentExpression(
      //           "starttime", new LiteralExpression(new ExprLongValue(1737964800))));
      //   arguments.add(
      //       new NamedArgumentExpression(
      //           "endtime", new LiteralExpression(new ExprLongValue(1738137600))));
      //   arguments.add(
      //       new NamedArgumentExpression("step", new LiteralExpression(new
      // ExprStringValue("1h"))));

      //   Table something =
      //       ((TableFunctionImplementation)
      //               queryRangeResolved.getRight().apply((FunctionProperties) null, arguments))
      //           .applyArguments();

      //   System.out.println("table: " + something);
      // } else {
      //   System.out.println("query_range function is not present");
      // }
    }

    String query = dispatchQueryRequest.getQuery();
    if (LangType.SQL.equals(dispatchQueryRequest.getLangType())) {
      if (SQLQueryUtils.isFlintExtensionQuery(query)) {
        sqlQueryValidator.validateFlintExtensionQuery(query, dataSourceMetadata.getConnector());
        return handleFlintExtensionQuery(
            dispatchQueryRequest, asyncQueryRequestContext, dataSourceMetadata);
      }

      sqlQueryValidator.validate(query, dataSourceMetadata.getConnector());
    } else if (LangType.PPL.equals(dispatchQueryRequest.getLangType())) {
      pplQueryValidator.validate(query, dataSourceMetadata.getConnector());
    }
    return handleDefaultQuery(dispatchQueryRequest, asyncQueryRequestContext, dataSourceMetadata);
  }

  private DispatchQueryResponse handleFlintExtensionQuery(
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext asyncQueryRequestContext,
      DataSourceMetadata dataSourceMetadata) {
    IndexQueryDetails indexQueryDetails = getIndexQueryDetails(dispatchQueryRequest);
    DispatchQueryContext context =
        getDefaultDispatchContextBuilder(
                dispatchQueryRequest, dataSourceMetadata, asyncQueryRequestContext)
            .indexQueryDetails(indexQueryDetails)
            .asyncQueryRequestContext(asyncQueryRequestContext)
            .build();

    return getQueryHandlerForFlintExtensionQuery(dispatchQueryRequest, indexQueryDetails)
        .submit(dispatchQueryRequest, context);
  }

  private DispatchQueryResponse handleDefaultQuery(
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext asyncQueryRequestContext,
      DataSourceMetadata dataSourceMetadata) {

    DispatchQueryContext context =
        getDefaultDispatchContextBuilder(
                dispatchQueryRequest, dataSourceMetadata, asyncQueryRequestContext)
            .asyncQueryRequestContext(asyncQueryRequestContext)
            .build();

    return getDefaultAsyncQueryHandler(dispatchQueryRequest.getAccountId())
        .submit(dispatchQueryRequest, context);
  }

  private DispatchQueryContext.DispatchQueryContextBuilder getDefaultDispatchContextBuilder(
      DispatchQueryRequest dispatchQueryRequest,
      DataSourceMetadata dataSourceMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    return DispatchQueryContext.builder()
        .dataSourceMetadata(dataSourceMetadata)
        .tags(getDefaultTagsForJobSubmission(dispatchQueryRequest))
        .queryId(queryIdProvider.getQueryId(dispatchQueryRequest, asyncQueryRequestContext));
  }

  private AsyncQueryHandler getQueryHandlerForFlintExtensionQuery(
      DispatchQueryRequest dispatchQueryRequest, IndexQueryDetails indexQueryDetails) {
    if (isEligibleForIndexDMLHandling(indexQueryDetails)) {
      return queryHandlerFactory.getIndexDMLHandler();
    } else if (isEligibleForStreamingQuery(indexQueryDetails)) {
      return queryHandlerFactory.getStreamingQueryHandler(dispatchQueryRequest.getAccountId());
    } else if (IndexQueryActionType.CREATE.equals(indexQueryDetails.getIndexQueryActionType())) {
      // Create should be handled by batch handler. This is to avoid DROP index incorrectly cancel
      // an interactive job.
      return queryHandlerFactory.getBatchQueryHandler(dispatchQueryRequest.getAccountId());
    } else if (IndexQueryActionType.REFRESH.equals(indexQueryDetails.getIndexQueryActionType())) {
      // Manual refresh should be handled by batch handler
      return queryHandlerFactory.getRefreshQueryHandler(dispatchQueryRequest.getAccountId());
    } else if (IndexQueryActionType.RECOVER.equals(indexQueryDetails.getIndexQueryActionType())) {
      // RECOVER INDEX JOB should not be executed from async-query-core
      throw new IllegalArgumentException("RECOVER INDEX JOB is not allowed.");
    } else {
      return getDefaultAsyncQueryHandler(dispatchQueryRequest.getAccountId());
    }
  }

  @NonNull
  private AsyncQueryHandler getDefaultAsyncQueryHandler(String accountId) {
    return sessionManager.isEnabled()
        ? queryHandlerFactory.getInteractiveQueryHandler()
        : queryHandlerFactory.getBatchQueryHandler(accountId);
  }

  @NonNull
  private static IndexQueryDetails getIndexQueryDetails(DispatchQueryRequest dispatchQueryRequest) {
    IndexQueryDetails indexQueryDetails =
        SQLQueryUtils.extractIndexDetails(dispatchQueryRequest.getQuery());
    fillDatasourceName(dispatchQueryRequest, indexQueryDetails);
    return indexQueryDetails;
  }

  private boolean isEligibleForStreamingQuery(IndexQueryDetails indexQueryDetails) {
    Boolean isCreateAutoRefreshIndex =
        IndexQueryActionType.CREATE.equals(indexQueryDetails.getIndexQueryActionType())
            && indexQueryDetails.getFlintIndexOptions().autoRefresh();
    Boolean isAlterQuery =
        IndexQueryActionType.ALTER.equals(indexQueryDetails.getIndexQueryActionType());
    return isCreateAutoRefreshIndex || isAlterQuery;
  }

  private boolean isEligibleForIndexDMLHandling(IndexQueryDetails indexQueryDetails) {
    return IndexQueryActionType.DROP.equals(indexQueryDetails.getIndexQueryActionType())
        || (IndexQueryActionType.ALTER.equals(indexQueryDetails.getIndexQueryActionType())
            && (indexQueryDetails
                    .getFlintIndexOptions()
                    .getProvidedOptions()
                    .containsKey("auto_refresh")
                && !indexQueryDetails.getFlintIndexOptions().autoRefresh()));
  }

  public JSONObject getQueryResponse(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    return getAsyncQueryHandlerForExistingQuery(asyncQueryJobMetadata)
        .getQueryResponse(asyncQueryJobMetadata, asyncQueryRequestContext);
  }

  public String cancelJob(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    return getAsyncQueryHandlerForExistingQuery(asyncQueryJobMetadata)
        .cancelJob(asyncQueryJobMetadata, asyncQueryRequestContext);
  }

  private AsyncQueryHandler getAsyncQueryHandlerForExistingQuery(
      AsyncQueryJobMetadata asyncQueryJobMetadata) {
    if (asyncQueryJobMetadata.getSessionId() != null) {
      return queryHandlerFactory.getInteractiveQueryHandler();
    } else if (IndexDMLHandler.isIndexDMLQuery(asyncQueryJobMetadata.getJobId())) {
      return queryHandlerFactory.getIndexDMLHandler();
    } else if (asyncQueryJobMetadata.getJobType() == JobType.REFRESH) {
      return queryHandlerFactory.getRefreshQueryHandler(asyncQueryJobMetadata.getAccountId());
    } else if (asyncQueryJobMetadata.getJobType() == JobType.STREAMING) {
      return queryHandlerFactory.getStreamingQueryHandler(asyncQueryJobMetadata.getAccountId());
    } else {
      return queryHandlerFactory.getBatchQueryHandler(asyncQueryJobMetadata.getAccountId());
    }
  }

  // TODO: Revisit this logic.
  // Currently, Spark if datasource is not provided in query.
  // Spark Assumes the datasource to be catalog.
  // This is required to handle drop index case properly when datasource name is not provided.
  private static void fillDatasourceName(
      DispatchQueryRequest dispatchQueryRequest, IndexQueryDetails indexQueryDetails) {
    if (indexQueryDetails.getFullyQualifiedTableName() != null
        && indexQueryDetails.getFullyQualifiedTableName().getDatasourceName() == null) {
      indexQueryDetails
          .getFullyQualifiedTableName()
          .setDatasourceName(dispatchQueryRequest.getDatasource());
    }
  }

  private static Map<String, String> getDefaultTagsForJobSubmission(
      DispatchQueryRequest dispatchQueryRequest) {
    Map<String, String> tags = new HashMap<>();
    tags.put(CLUSTER_NAME_TAG_KEY, dispatchQueryRequest.getClusterName());
    tags.put(DATASOURCE_TAG_KEY, dispatchQueryRequest.getDatasource());
    return tags;
  }

  private OkHttpClient getHttpClient() {
    OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();
    okHttpClient.callTimeout(1, TimeUnit.MINUTES);
    okHttpClient.connectTimeout(30, TimeUnit.SECONDS);
    okHttpClient.followRedirects(false);
    okHttpClient.addInterceptor(new URIValidatorInterceptor(new ArrayList<>()));
    return okHttpClient.build();
  }
}
