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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tests.integration.containers.ElasticSearchContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

@Slf4j
public class ElasticSearchSinkTester extends SinkTester<ElasticSearchContainer> {

    private RestHighLevelClient elasticClient;
    private boolean schemaEnable;
    private final Schema<KeyValue<SimplePojo, SimplePojo>> kvSchema;

    @Data
    @AllArgsConstructor
    public static final class SimplePojo {
        private String field1;
        private String field2;
    }

    /**
     * This method is used to pre create the subscription for the Sink.
     * @return the schema for the subscription
     */
    public Schema<?> getInputTopicSchema() {
        if (schemaEnable) {
            // we do not want to enforce a Schema
            // at the beginning of the test
            return Schema.AUTO_CONSUME();
        } else {
            return Schema.STRING;
        }
    }

    public ElasticSearchSinkTester(boolean schemaEnable) {
        super(ElasticSearchContainer.NAME, SinkType.ELASTIC_SEARCH);

        sinkConfig.put("elasticSearchUrl", "http://" + ElasticSearchContainer.NAME + ":9200");
        sinkConfig.put("indexName", "test-index");
        this.schemaEnable = schemaEnable;
        if (schemaEnable) {
            sinkConfig.put("schemaEnable", "true");
            kvSchema = Schema.KeyValue(Schema.JSON(SimplePojo.class),
                    Schema.AVRO(SimplePojo.class), KeyValueEncodingType.SEPARATED);
        } else {
            // default behaviour, it must be enabled the default, in order to preserve compatibility with Pulsar 2.8.x
            kvSchema = null;
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
    public void stopServiceContainer(PulsarCluster cluster) {
        try {
            if (elasticClient != null) {
                elasticClient.close();
            }
        } catch (IOException e) {
            log.warn("Error closing elasticClient, ignoring", e);
        } finally {
            super.stopServiceContainer(cluster);
        }
    }

    @Override
    public void validateSinkResult(Map<String, String> kvs) {
        SearchRequest searchRequest = new SearchRequest("test-index");

        Awaitility.await().untilAsserted(() -> {
            SearchResponse searchResult = elasticClient.search(searchRequest, RequestOptions.DEFAULT);
            assertTrue(searchResult.getHits().getTotalHits().value > 0, searchResult.toString());
        });
    }

    @Override
    public void produceMessage(int numMessages, PulsarClient client,
                               String inputTopicName, LinkedHashMap<String, String> kvs) throws Exception {
        if (schemaEnable) {

            @Cleanup
            Producer<KeyValue<SimplePojo, SimplePojo>> producer = client.newProducer(kvSchema)
                    .topic(inputTopicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                String key = "key-" + i;
                kvs.put(key, key);
                producer.newMessage()
                        .value(new KeyValue<>(new SimplePojo("f1_" + i, "f2_" + i),
                                new SimplePojo("v1_" + i, "v2_" + i)))
                        .send();
            }

        } else {

            @Cleanup
            Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(inputTopicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                String key = "key-" + i;
                // this is a JSON document, written to ElasticSearch
                Map<String, String> valueMap = new HashMap<>();
                valueMap.put("key" + i, "value" + i);
                String value = ObjectMapperFactory.getThreadLocal().writeValueAsString(valueMap);
                kvs.put(key, value);
                producer.newMessage()
                        .key(key)
                        .value(value)
                        .send();
            }

        }
    }

}
