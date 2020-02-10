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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * The base abstract class for ElasticSearch sinks.
 * Users need to implement extractKeyValue function to use this sink.
 * This class assumes that the input will be JSON documents
 */
@Connector(
    name = "elastic_search",
    type = IOType.SINK,
    help = "A sink connector that sends pulsar messages to elastic search",
    configClass = ElasticSearchConfig.class
)
public class ElasticSearchSink implements Sink<byte[]> {

    private URL url;
    private RestHighLevelClient client;
    private CredentialsProvider credentialsProvider;
    private ElasticSearchConfig elasticSearchConfig;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        elasticSearchConfig = ElasticSearchConfig.load(config);
        elasticSearchConfig.validate();
        createIndexIfNeeded();
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void write(Record<byte[]> record) {
        KeyValue<String, byte[]> keyValue = extractKeyValue(record);
        IndexRequest indexRequest = Requests.indexRequest(elasticSearchConfig.getIndexName());
        indexRequest.type(elasticSearchConfig.getTypeName());
        indexRequest.source(keyValue.getValue(), XContentType.JSON);

        try {
        IndexResponse indexResponse = getClient().index(indexRequest);
            if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
                record.ack();
            } else {
                record.fail();
            }
        } catch (final IOException ex) {
            record.fail();
        }
    }

    public KeyValue<String, byte[]> extractKeyValue(Record<byte[]> record) {
        String key = record.getKey().orElse("");
        return new KeyValue<>(key, record.getValue());
    }

    private void createIndexIfNeeded() throws IOException {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(elasticSearchConfig.getIndexName());
        boolean exists = getClient().indices().exists(request);

        if (!exists) {
            CreateIndexRequest cireq = new CreateIndexRequest(elasticSearchConfig.getIndexName());

            cireq.settings(Settings.builder()
               .put("index.number_of_shards", elasticSearchConfig.getIndexNumberOfShards())
               .put("index.number_of_replicas", elasticSearchConfig.getIndexNumberOfReplicas()));

            CreateIndexResponse ciresp = getClient().indices().create(cireq);
            if (!ciresp.isAcknowledged() || !ciresp.isShardsAcknowledged()) {
                throw new RuntimeException("Unable to create index.");
            }
        }
    }

    private URL getUrl() throws MalformedURLException {
        if (url == null) {
            url = new URL(elasticSearchConfig.getElasticSearchUrl());
        }
        return url;
    }

    private CredentialsProvider getCredentialsProvider() {

        if (StringUtils.isEmpty(elasticSearchConfig.getUsername())
            || StringUtils.isEmpty(elasticSearchConfig.getPassword())) {
            return null;
        }

        credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(elasticSearchConfig.getUsername(),
                        elasticSearchConfig.getPassword()));
        return credentialsProvider;
    }

    private RestHighLevelClient getClient() throws MalformedURLException {
        if (client == null) {
          CredentialsProvider cp = getCredentialsProvider();
          RestClientBuilder builder = RestClient.builder(new HttpHost(getUrl().getHost(),
                  getUrl().getPort(), getUrl().getProtocol()));

          if (cp != null) {
              builder.setHttpClientConfigCallback(httpClientBuilder ->
              httpClientBuilder.setDefaultCredentialsProvider(cp));
          }
          client = new RestHighLevelClient(builder);
        }
        return client;
    }
}
