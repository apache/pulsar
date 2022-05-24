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
import java.util.Map;
import java.util.Optional;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.security.CreateApiKeyRequest;
import co.elastic.clients.elasticsearch.security.CreateApiKeyResponse;
import co.elastic.clients.elasticsearch.security.GetTokenRequest;
import co.elastic.clients.elasticsearch.security.GetTokenResponse;
import co.elastic.clients.elasticsearch.security.get_token.AccessTokenGrantType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.io.elasticsearch.client.elastic.ElasticSearchJavaRestClient;
import org.apache.pulsar.io.elasticsearch.client.opensearch.OpenSearchHighLevelRestClient;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class ElasticSearchTestBase {

    public static final String ELASTICSEARCH_8 = Optional.ofNullable(System.getenv("ELASTICSEARCH_IMAGE_V8"))
            .orElse("docker.elastic.co/elasticsearch/elasticsearch:8.1.0");

    public static final String ELASTICSEARCH_7 = Optional.ofNullable(System.getenv("ELASTICSEARCH_IMAGE_V7"))
            .orElse("docker.elastic.co/elasticsearch/elasticsearch:7.16.3-amd64");

    public static final String OPENSEARCH = Optional.ofNullable(System.getenv("OPENSEARCH_IMAGE"))
            .orElse("opensearchproject/opensearch:1.2.4");

    protected final String elasticImageName;

    public ElasticSearchTestBase(String elasticImageName) {
        this.elasticImageName = elasticImageName;
    }

    protected ElasticsearchContainer createElasticsearchContainer() {
        if (elasticImageName.equals(OPENSEARCH)) {
            DockerImageName dockerImageName = DockerImageName.parse(OPENSEARCH).asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");
            return new ElasticsearchContainer(dockerImageName)
                    .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms128m -Xmx256m")
                    .withEnv("bootstrap.memory_lock", "true")
                    .withEnv("plugins.security.disabled", "true");
        }
        return new ElasticsearchContainer(elasticImageName)
                .withEnv("ES_JAVA_OPTS", "-Xms128m -Xmx256m")
                .withEnv("xpack.security.enabled", "false")
                .withEnv("xpack.security.http.ssl.enabled", "false");
    }

    protected ElasticSearchConfig.CompatibilityMode getCompatibilityMode() {
        if (elasticImageName.equals(ELASTICSEARCH_7)) {
            return ElasticSearchConfig.CompatibilityMode.ELASTICSEARCH_7;
        } else if (elasticImageName.equals(ELASTICSEARCH_8)) {
            return ElasticSearchConfig.CompatibilityMode.ELASTICSEARCH;
        } else if (elasticImageName.equals(OPENSEARCH)) {
            return ElasticSearchConfig.CompatibilityMode.OPENSEARCH;
        }
        throw new IllegalStateException("unexpected image: " + elasticImageName);
    }

    protected String createAuthToken(ElasticSearchClient client, String username, String pwd) throws IOException {
        if (elasticImageName.equals(ELASTICSEARCH_8)) {
            final ElasticSearchJavaRestClient restClient = (ElasticSearchJavaRestClient)
                    client.getRestClient();
            ElasticsearchClient lowLevelClient = restClient.getClient();
            final GetTokenResponse response = lowLevelClient.security()
                    .getToken(
                            new GetTokenRequest.Builder()
                                    .grantType(AccessTokenGrantType.Password)
                                    .username(username)
                                    .password(pwd)
                                    .build());
            return response.accessToken();
        } else {
            final OpenSearchHighLevelRestClient restClient = (OpenSearchHighLevelRestClient)
                    client.getRestClient();
            final Request post = new Request("POST", "/_security/oauth2/token");
            post.setJsonEntity("{\"grant_type\":\"client_credentials\"}");
            final Response response = restClient.getClient().getLowLevelClient().performRequest(post);

            final Map map = new ObjectMapper().readValue(response.getEntity().getContent(), Map.class);
            return (String) map.get("access_token");
        }

    }

    protected String createApiKey(ElasticSearchClient client) throws IOException  {
        if (elasticImageName.equals(ELASTICSEARCH_8)) {
            final ElasticSearchJavaRestClient restClient = (ElasticSearchJavaRestClient)
                    client.getRestClient();
            ElasticsearchClient lowLevelClient = restClient.getClient();

            final CreateApiKeyResponse response = lowLevelClient.security().createApiKey(new CreateApiKeyRequest.Builder().name("api-key").build());
            return response.encoded();
        } else {
            final OpenSearchHighLevelRestClient restClient = (OpenSearchHighLevelRestClient)
                    client.getRestClient();
            final Request post = new Request("POST", "/_security/api_key");
            post.setJsonEntity("{\"name\":\"api-key\"}");
            final Response response = restClient.getClient().getLowLevelClient().performRequest(post);
            final Map map = new ObjectMapper().readValue(response.getEntity().getContent(), Map.class);
            return (String) map.get("encoded");
        }

    }
}
