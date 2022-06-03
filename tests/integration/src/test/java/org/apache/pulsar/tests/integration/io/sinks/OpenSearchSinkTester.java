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
package org.apache.pulsar.tests.integration.io.sinks;

import org.apache.http.HttpHost;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static org.testng.Assert.assertTrue;

public class OpenSearchSinkTester extends ElasticSearchSinkTester {

    private RestHighLevelClient elasticClient;


    public OpenSearchSinkTester(boolean schemaEnable) {
        super(schemaEnable);
    }

    @Override
    protected ElasticsearchContainer createSinkService(PulsarCluster cluster) {
        DockerImageName dockerImageName = DockerImageName.parse("opensearchproject/opensearch:1.2.4")
                .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");
        return new ElasticsearchContainer(dockerImageName)
                .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms128m -Xmx256m")
                .withEnv("bootstrap.memory_lock", "true")
                .withEnv("plugins.security.disabled", "true");
    }

    @Override
    public void prepareSink() throws Exception {
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(
                        "localhost",
                        serviceContainer.getMappedPort(9200),
                        "http"));
        elasticClient = new RestHighLevelClient(builder);
    }

    @Override
    public void validateSinkResult(Map<String, String> kvs) {
        org.opensearch.action.search.SearchRequest searchRequest = new SearchRequest("test-index");

        Awaitility.await().untilAsserted(() -> {
            SearchResponse searchResult = elasticClient.search(searchRequest, RequestOptions.DEFAULT);
            assertTrue(searchResult.getHits().getTotalHits().value > 0, searchResult.toString());
        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (elasticClient != null) {
            elasticClient.close();
            elasticClient = null;
        }
    }
}
