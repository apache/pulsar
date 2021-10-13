/**
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
package org.apache.pulsar.io.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ElasticSearchClient implements AutoCloseable {

    static final String[] malformedErrors = {
            "mapper_parsing_exception",
            "action_request_validation_exception",
            "illegal_argument_exception"
    };

    private ElasticSearchConfig config;
    private ConfigCallback configCallback;
    private RestHighLevelClient client;

    final Set<String> indexCache = new HashSet<>();
    final Map<String, String> topicToIndexCache = new HashMap<>();

    final RandomExponentialRetry backoffRetry;
    final BulkProcessor bulkProcessor;
    final ConcurrentMap<DocWriteRequest<?>, Record> records = new ConcurrentHashMap<>();
    final AtomicReference<Exception> irrecoverableError = new AtomicReference<>();
    final ScheduledExecutorService executorService;

    ElasticSearchClient(ElasticSearchConfig elasticSearchConfig) {
        this.config = elasticSearchConfig;
        this.configCallback = new ConfigCallback();
        this.backoffRetry = new RandomExponentialRetry(elasticSearchConfig.getMaxRetryTimeInSec());
        if (!config.isBulkEnabled()) {
            bulkProcessor = null;
        } else {
            BulkProcessor.Builder builder = BulkProcessor.builder(
                    (bulkRequest, bulkResponseActionListener) -> client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener),
                    new BulkProcessor.Listener() {
                        @Override
                        public void beforeBulk(long l, BulkRequest bulkRequest) {
                        }

                        @Override
                        public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                            log.trace("Bulk request id={} size={}:", l, bulkRequest.requests().size());
                            for (int i = 0; i < bulkResponse.getItems().length; i++) {
                                DocWriteRequest<?> request = bulkRequest.requests().get(i);
                                Record record = records.get(request);
                                BulkItemResponse bulkItemResponse = bulkResponse.getItems()[i];
                                if (bulkItemResponse.isFailed()) {
                                    record.fail();
                                    try {
                                        hasIrrecoverableError(bulkItemResponse);
                                    } catch(Exception e) {
                                        log.warn("Unrecoverable error:", e);
                                    }
                                } else {
                                    record.ack();
                                }
                                records.remove(request);
                            }
                        }

                        @Override
                        public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                            log.warn("Bulk request id={} failed:", l, throwable);
                            for (DocWriteRequest<?> request : bulkRequest.requests()) {
                                Record record = records.remove(request);
                                record.fail();
                            }
                        }
                    }
            )
                    .setBulkActions(config.getBulkActions())
                    .setBulkSize(new ByteSizeValue(config.getBulkSizeInMb(), ByteSizeUnit.MB))
                    .setConcurrentRequests(config.getBulkConcurrentRequests())
                    .setBackoffPolicy(new RandomExponentialBackoffPolicy(backoffRetry,
                            config.getRetryBackoffInMs(),
                            config.getMaxRetries()
                    ));
            if (config.getBulkFlushIntervalInMs() > 0) {
                builder.setFlushInterval(new TimeValue(config.getBulkFlushIntervalInMs(), TimeUnit.MILLISECONDS));
            }
            this.bulkProcessor = builder.build();
        }

        // idle+expired connection evictor thread
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.executorService.scheduleAtFixedRate(new Runnable() {
                                                     @Override
                                                     public void run() {
                                                         configCallback.connectionManager.closeExpiredConnections();
                                                         configCallback.connectionManager.closeIdleConnections(
                                                                 config.getConnectionIdleTimeoutInMs(), TimeUnit.MILLISECONDS);
                                                     }
                                                 },
                config.getConnectionIdleTimeoutInMs(),
                config.getConnectionIdleTimeoutInMs(),
                TimeUnit.MILLISECONDS
        );

        log.info("ElasticSearch URL {}", config.getElasticSearchUrl());
        HttpHost[] hosts = getHttpHosts(config);
        RestClientBuilder builder = RestClient.builder(hosts)
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
                        return builder
                                .setContentCompressionEnabled(config.isCompressionEnabled())
                                .setConnectionRequestTimeout(config.getConnectionRequestTimeoutInMs())
                                .setConnectTimeout(config.getConnectTimeoutInMs())
                                .setSocketTimeout(config.getSocketTimeoutInMs());
                    }
                })
                .setHttpClientConfigCallback(this.configCallback)
                .setFailureListener(new RestClient.FailureListener() {
                    public void onFailure(Node node) {
                        log.warn("Node host={} failed", node.getHost());
                    }
                });
        this.client = new RestHighLevelClient(builder);
    }

    void failed(Exception e) throws Exception {
        if (irrecoverableError.compareAndSet(null, e)) {
            log.error("Irrecoverable error:", e);
        }
    }

    boolean isFailed() {
        return irrecoverableError.get() != null;
    }

    void hasIrrecoverableError(BulkItemResponse bulkItemResponse) throws Exception {
        for (String error : malformedErrors) {
            if (bulkItemResponse.getFailureMessage().contains(error)) {
                switch (config.getMalformedDocAction()) {
                    case IGNORE:
                        break;
                    case WARN:
                        log.warn("Ignoring malformed document index={} id={}",
                                bulkItemResponse.getIndex(),
                                bulkItemResponse.getId(),
                                bulkItemResponse.getFailure().getCause());
                        break;
                    case FAIL:
                        log.error("Failure due to the malformed document index={} id={}",
                                bulkItemResponse.getIndex(),
                                bulkItemResponse.getId(),
                                bulkItemResponse.getFailure().getCause());
                        failed(bulkItemResponse.getFailure().getCause());
                        break;
                }
            }
        }
    }

    public void bulkIndex(Record record, Pair<String, String> idAndDoc) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record.getTopicName());
            IndexRequest indexRequest = Requests.indexRequest(config.getIndexName());
            if (!Strings.isNullOrEmpty(idAndDoc.getLeft()))
                indexRequest.id(idAndDoc.getLeft());
            indexRequest.type(config.getTypeName());
            indexRequest.source(idAndDoc.getRight(), XContentType.JSON);

            records.put(indexRequest, record);
            bulkProcessor.add(indexRequest);
        } catch(Exception e) {
            log.debug("index failed id=" + idAndDoc.getLeft(), e);
            record.fail();
            throw e;
        }
    }

    /**
     * Index an elasticsearch document and ack the record.
     * @param record
     * @param idAndDoc
     * @return
     * @throws Exception
     */
    public boolean indexDocument(Record<GenericObject> record, Pair<String, String> idAndDoc) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record.getTopicName());
            IndexRequest indexRequest = Requests.indexRequest(config.getIndexName());
            if (!Strings.isNullOrEmpty(idAndDoc.getLeft()))
                indexRequest.id(idAndDoc.getLeft());
            indexRequest.type(config.getTypeName());
            indexRequest.source(idAndDoc.getRight(), XContentType.JSON);
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED) ||
                    indexResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
                record.ack();
                return true;
            } else {
                record.fail();
                return false;
            }
        } catch (final Exception ex) {
            log.error("index failed id=" + idAndDoc.getLeft(), ex);
            record.fail();
            throw ex;
        }
    }

    public void bulkDelete(Record<GenericObject> record, String id) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record.getTopicName());
            DeleteRequest deleteRequest = Requests.deleteRequest(config.getIndexName());
            deleteRequest.id(id);
            deleteRequest.type(config.getTypeName());

            records.put(deleteRequest, record);
            bulkProcessor.add(deleteRequest);
        } catch(Exception e) {
            log.debug("delete failed id=" + id, e);
            record.fail();
            throw e;
        }
    }

    /**
     * Delete an elasticsearch document and ack the record.
     * @param record
     * @param id
     * @return
     * @throws IOException
     */
    public boolean deleteDocument(Record<GenericObject> record, String id) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record.getTopicName());
            DeleteRequest deleteRequest = Requests.deleteRequest(config.getIndexName());
            deleteRequest.id(id);
            deleteRequest.type(config.getTypeName());
            DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            log.debug("delete result=" + deleteResponse.getResult());
            if (deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED) ||
                    deleteResponse.getResult().equals(DocWriteResponse.Result.NOT_FOUND)) {
                record.ack();
                return true;
            }
            record.fail();
            return false;
        } catch (final Exception ex) {
            log.debug("index failed id=" + id, ex);
            record.fail();
            throw ex;
        }
    }

    /**
     * Flushes the bulk processor.
     */
    public void flush() {
        bulkProcessor.flush();
    }

    @Override
    public void close() {
        try {
            if (bulkProcessor != null) {
                bulkProcessor.awaitClose(5000L, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            log.warn("Elasticsearch bulk processor close error:", e);
        }
        try {
            this.executorService.shutdown();
            if (this.client != null) {
                this.client.close();
            }
        } catch (IOException e) {
            log.warn("Elasticsearch client close error:", e);
        }
    }

    private void checkNotFailed() throws Exception {
        if (irrecoverableError.get() != null) {
            throw irrecoverableError.get();
        }
    }

    private void checkIndexExists(Optional<String> topicName) throws IOException {
        if (!config.isCreateIndexIfNeeded()) {
            return;
        }
        String indexName = indexName(topicName);
        if (!indexCache.contains(indexName)) {
            synchronized (this) {
                if (!indexCache.contains(indexName)) {
                    createIndexIfNeeded(indexName);
                    indexCache.add(indexName);
                }
            }
        }
    }

    private String indexName(Optional<String> topicName) throws IOException {
        if (config.getIndexName() != null) {
            // Use the configured indexName if provided.
            return config.getIndexName();
        }
        if (!topicName.isPresent()) {
            throw new IOException("Elasticsearch index name configuration and topic name are empty");
        }
        return topicToIndexName(topicName.get());
    }

    @VisibleForTesting
    public String topicToIndexName(String topicName) {
        return topicToIndexCache.computeIfAbsent(topicName, k -> {
            // see elasticsearch limitations https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params
            String indexName = topicName.toLowerCase(Locale.ROOT);

            // remove the pulsar topic info persistent://tenant/namespace/topic
            String[] parts = indexName.split("/");
            if (parts.length > 1) {
                indexName = parts[parts.length-1];
            }

            // truncate to the max bytes length
            while (indexName.getBytes(StandardCharsets.UTF_8).length > 255) {
                indexName = indexName.substring(0, indexName.length() - 1);
            }
            if (indexName.length() <= 0 || !indexName.matches("[a-zA-Z\\.0-9][a-zA-Z_\\.\\-\\+0-9]*")) {
                throw new RuntimeException(new IOException("Cannot convert the topic name='" + topicName + "' to a valid elasticsearch index name"));
            }
            if (log.isDebugEnabled()) {
                log.debug("Translate topic={} to index={}", k, indexName);
            }
            return indexName;
        });
    }

    @VisibleForTesting
    public boolean createIndexIfNeeded(String indexName) throws IOException {
        if (indexExists(indexName)) {
            return false;
        }
        final CreateIndexRequest cireq = new CreateIndexRequest(indexName);
        cireq.settings(Settings.builder()
                .put("index.number_of_shards", config.getIndexNumberOfShards())
                .put("index.number_of_replicas", config.getIndexNumberOfReplicas()));
        return retry(() -> {
            CreateIndexResponse resp = client.indices().create(cireq, RequestOptions.DEFAULT);
            if (!resp.isAcknowledged() || !resp.isShardsAcknowledged()) {
                throw new IOException("Unable to create index.");
            }
            return true;
        }, "create index");
    }

    public boolean indexExists(final String indexName) throws IOException {
        final GetIndexRequest request = new GetIndexRequest(indexName);
        return retry(() -> client.indices().exists(request, RequestOptions.DEFAULT), "index exists");
    }

    @VisibleForTesting
    protected long totalHits(String indexName) throws IOException {
        return search(indexName).getHits().getTotalHits().value;
    }

    @VisibleForTesting
    protected org.elasticsearch.action.search.SearchResponse search(String indexName) throws IOException {
        client.indices().refresh(new RefreshRequest(indexName), RequestOptions.DEFAULT);
        return client.search(
                new SearchRequest()
                        .indices(indexName)
                        .source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())),
                RequestOptions.DEFAULT);
    }

    @VisibleForTesting
    protected org.elasticsearch.action.admin.indices.refresh.RefreshResponse refresh(String indexName) throws IOException {
        return client.indices().refresh(new RefreshRequest(indexName), RequestOptions.DEFAULT);
    }

    @VisibleForTesting
    protected org.elasticsearch.action.support.master.AcknowledgedResponse delete(String indexName) throws IOException {
        return client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
    }

    private <T> T retry(Callable<T> callable, String source) {
        try {
            return backoffRetry.retry(callable, config.getMaxRetries(), config.getRetryBackoffInMs(), source);
        } catch (Exception e) {
            log.error("error in command {} wth retry", source, e);
            throw new ElasticSearchConnectionException(source + " failed", e);
        }
    }

    public class ConfigCallback implements RestClientBuilder.HttpClientConfigCallback {
        final NHttpClientConnectionManager connectionManager;
        final CredentialsProvider credentialsProvider;

        public ConfigCallback() {
            this.connectionManager = buildConnectionManager(ElasticSearchClient.this.config);
            this.credentialsProvider = buildCredentialsProvider(ElasticSearchClient.this.config);
        }

        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder builder) {
            builder.setMaxConnPerRoute(config.getBulkConcurrentRequests());
            builder.setMaxConnTotal(config.getBulkConcurrentRequests());
            builder.setConnectionManager(connectionManager);

            if (this.credentialsProvider != null) {
                builder.setDefaultCredentialsProvider(credentialsProvider);
            }
            return builder;
        }

        public NHttpClientConnectionManager buildConnectionManager(ElasticSearchConfig config) {
            try {
                IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                        .setConnectTimeout(config.getConnectTimeoutInMs())
                        .setSoTimeout(config.getSocketTimeoutInMs())
                        .build();
                ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
                PoolingNHttpClientConnectionManager connManager;
                if (config.getSsl().isEnabled()) {
                    ElasticSearchSslConfig sslConfig = config.getSsl();
                    HostnameVerifier hostnameVerifier = config.getSsl().isHostnameVerification()
                            ? SSLConnectionSocketFactory.getDefaultHostnameVerifier()
                            : new NoopHostnameVerifier();
                    String[] cipherSuites = null;
                    if (!Strings.isNullOrEmpty(sslConfig.getCipherSuites())) {
                        cipherSuites = sslConfig.getCipherSuites().split(",");
                    }
                    String[] protocols = null;
                    if (!Strings.isNullOrEmpty(sslConfig.getProtocols())) {
                        protocols = sslConfig.getProtocols().split(",");
                    }
                    Registry<SchemeIOSessionStrategy> registry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                            .register("http", NoopIOSessionStrategy.INSTANCE)
                            .register("https", new SSLIOSessionStrategy(
                                    buildSslContext(config),
                                    protocols,
                                    cipherSuites,
                                    hostnameVerifier))
                            .build();
                    connManager = new PoolingNHttpClientConnectionManager(ioReactor, registry);
                } else {
                    connManager = new PoolingNHttpClientConnectionManager(ioReactor);
                }
                return connManager;
            } catch(Exception e) {
                throw new ElasticSearchConnectionException(e);
            }
        }

        private SSLContext buildSslContext(ElasticSearchConfig config) throws NoSuchAlgorithmException, KeyManagementException, CertificateException, KeyStoreException, IOException, UnrecoverableKeyException {
            ElasticSearchSslConfig sslConfig = config.getSsl();
            SSLContextBuilder sslContextBuilder = SSLContexts.custom();
            if (!Strings.isNullOrEmpty(sslConfig.getProvider())) {
                sslContextBuilder.setProvider(sslConfig.getProvider());
            }
            if (!Strings.isNullOrEmpty(sslConfig.getProtocols())) {
                sslContextBuilder.setProtocol(sslConfig.getProtocols());
            }
            if (!Strings.isNullOrEmpty(sslConfig.getTruststorePath()) && !Strings.isNullOrEmpty(sslConfig.getTruststorePassword())) {
                sslContextBuilder.loadTrustMaterial(new File(sslConfig.getTruststorePath()), sslConfig.getTruststorePassword().toCharArray());
            }
            if (!Strings.isNullOrEmpty(sslConfig.getKeystorePath()) && !Strings.isNullOrEmpty(sslConfig.getKeystorePassword())) {
                sslContextBuilder.loadKeyMaterial(new File(sslConfig.getKeystorePath()),
                        sslConfig.getKeystorePassword().toCharArray(),
                        sslConfig.getKeystorePassword().toCharArray());
            }
            return sslContextBuilder.build();
        }

        private CredentialsProvider buildCredentialsProvider(ElasticSearchConfig config) {
            if (StringUtils.isEmpty(config.getUsername()) || StringUtils.isEmpty(config.getPassword())) {
                return null;
            }
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            return credentialsProvider;
        }
    }


    private static HttpHost[] getHttpHosts(ElasticSearchConfig elasticSearchConfig) {
        String url = elasticSearchConfig.getElasticSearchUrl();
        return Arrays.stream(url.split(",")).map(host -> {
            try {
                URL hostUrl = new URL(host);
                return new HttpHost(hostUrl.getHost(), hostUrl.getPort(),
                        hostUrl.getProtocol());
            } catch (MalformedURLException e) {
                throw new RuntimeException("Invalid elasticSearch url :" + host);
            }
        }).toArray(HttpHost[]::new);
    }

    RestHighLevelClient getClient() {
        return client;
    }
}
