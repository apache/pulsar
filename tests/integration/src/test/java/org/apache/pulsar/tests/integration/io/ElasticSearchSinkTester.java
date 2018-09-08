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
package org.apache.pulsar.tests.integration.io;

import static com.google.common.base.Preconditions.checkState;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.pulsar.tests.integration.containers.ElasticSearchContainer;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.containers.GenericContainer;

public class ElasticSearchSinkTester extends SinkTester {
    
    private RestHighLevelClient elasticClient;

    public ElasticSearchSinkTester() {
        super(SinkType.ELASTIC_SEARCH);
        
        sinkConfig.put("elasticSearchUrl", "http://localhost:9200");
        sinkConfig.put("indexName", "test-index");
    }

    @Override
    public void findSinkServiceContainer(Map<String, GenericContainer<?>> externalServices) {
        GenericContainer<?> container = externalServices.get(ElasticSearchContainer.NAME);
        checkState(container instanceof ElasticSearchContainer,
            "No ElasticSearch service found in the cluster");
    }

    @Override
    public void prepareSink() throws Exception {
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        elasticClient = new RestHighLevelClient(builder);
    }

    @Override
    public void validateSinkResult(Map<String, String> kvs) {
        
        SearchRequest searchRequest = new SearchRequest("test-index");
        searchRequest.types("doc");
        
        try {
            Header headers = null;
            SearchResponse searchResult = elasticClient.search(searchRequest, headers);
            assertTrue(searchResult.getHits().getTotalHits() > 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
