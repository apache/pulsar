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

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.LinkedHashMap;
import java.util.Map;

import lombok.Data;
import org.apache.http.HttpHost;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.tests.integration.containers.ElasticSearchContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchSinkTester extends SinkTester<ElasticSearchContainer> {

    private RestHighLevelClient elasticClient;
    private boolean schemaEnable;
    private final Schema<?> schema;

    @Data
    public static final class ValuePojo {
        private String field1;
        private String field2;
    }

    public ElasticSearchSinkTester(boolean schemaEnable) {
        super(ElasticSearchContainer.NAME, SinkType.ELASTIC_SEARCH);
        
        sinkConfig.put("elasticSearchUrl", "http://" + ElasticSearchContainer.NAME + ":9200");
        sinkConfig.put("indexName", "test-index");
        this.schemaEnable = schemaEnable;
        if (schemaEnable) {
            sinkConfig.put("schemaEnable", "true");
            schema = Schema.KeyValue(Schema.JSON(ValuePojo.class), Schema.AVRO(ValuePojo.class), KeyValueEncodingType.SEPARATED);
        } else {
            // default behaviour, it must be enabled the default, in order to preserve compatibility with Pulsar 2.8.x
            schema = Schema.STRING;
        }
    }


    @Override
    protected ElasticSearchContainer createSinkService(PulsarCluster cluster) {
        return new ElasticSearchContainer(cluster.getClusterName());
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
        SearchRequest searchRequest = new SearchRequest("test-index");
        searchRequest.types("doc");
        
        try {
            SearchResponse searchResult = elasticClient.search(searchRequest, RequestOptions.DEFAULT);
            assertTrue(searchResult.getHits().getTotalHits().value > 0, searchResult.toString());
        } catch (Exception e) {
            fail("Encountered exception on validating elastic search results", e);
        }
    }

    public void produceMessage(int i, Producer<String> producer, LinkedHashMap<String, String> kvs) throws PulsarClientException {
        if (schemaEnable) {
            String key = "key-" + i;
            // this is a JSON document, written to ElasticSearch
            String value = "{\"key" + i + "\"=\"value\"";
            kvs.put(key, value);
            producer.newMessage(schema)
                    .value(new KeyValue<>(new ValuePojo(), new ValuePojo());
                    .send();
        } else {
            String key = "key-" + i;
            // this is a JSON document, written to ElasticSearch
            String value = "{\"key" + i + "\"=\"value\"";
            kvs.put(key, value);
            producer.newMessage()
                    .key(key)
                    .value(value)
                    .send();
        }
    }

}
