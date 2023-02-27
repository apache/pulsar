/*
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.google.common.collect.ImmutableMap;
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
import org.awaitility.Awaitility;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

@Slf4j
public abstract class ElasticSearchSinkTester extends SinkTester<ElasticsearchContainer> {

    private static final String NAME = "elastic-search";

    private ElasticsearchClient elasticClient;
    private boolean schemaEnable;
    private final Schema<KeyValue<SimplePojo, SimplePojo>> kvSchema;

    @Data
    @AllArgsConstructor
    public static final class SimplePojo {
        private String field1;
        private String field2;
        private List<Integer> list1;
        private Set<Long> set1;
        private Map<String, String> map1;
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
        super(NAME, SinkType.ELASTIC_SEARCH);

        sinkConfig.put("elasticSearchUrl", "http://" + NAME + ":9200");
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
    public void prepareSink() throws Exception {
        RestClientBuilder builder = RestClient.builder(
            new HttpHost(
                "localhost",
                serviceContainer.getMappedPort(9200),
                "http"));
        ElasticsearchTransport transport = new RestClientTransport(builder.build(),
                new JacksonJsonpMapper());
        elasticClient = new ElasticsearchClient(transport);
    }

    @Override
    public void validateSinkResult(Map<String, String> kvs) {
        Awaitility.await().untilAsserted(() -> {
            SearchResponse<?> searchResult = elasticClient.search(new SearchRequest.Builder().index("test-index")
                    .q("*:*")
                    .build(), Map.class);
            assertTrue(searchResult.hits().total().value() > 0, searchResult.toString());
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
                final SimplePojo keyPojo = new SimplePojo(
                        "f1_" + i,
                        "f2_" + i,
                        Arrays.asList(i, i +1),
                        new HashSet<>(Arrays.asList((long) i)),
                        ImmutableMap.of("map1_k_" + i, "map1_kv_" + i));
                final SimplePojo valuePojo = new SimplePojo(
                        "f1_" + i,
                        "f2_" + i,
                        Arrays.asList(i, i +1),
                        new HashSet<>(Arrays.asList((long) i)),
                        ImmutableMap.of("map1_v_" + i, "map1_vv_" + i));
                producer.newMessage()
                        .value(new KeyValue<>(keyPojo, valuePojo))
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
                String value = ObjectMapperFactory.getMapper().getObjectMapper().writeValueAsString(valueMap);
                kvs.put(key, value);
                producer.newMessage()
                        .key(key)
                        .value(value)
                        .send();
            }

        }
    }

    @Override
    public void close() throws Exception {
        if (elasticClient != null) {
            elasticClient._transport().close();
            elasticClient = null;
        }
    }
}
