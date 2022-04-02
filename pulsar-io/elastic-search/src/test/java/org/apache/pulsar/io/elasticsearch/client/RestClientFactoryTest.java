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
package org.apache.pulsar.io.elasticsearch.client;

import lombok.SneakyThrows;
import org.apache.pulsar.io.elasticsearch.ElasticSearchConfig;
import org.apache.pulsar.io.elasticsearch.client.elastic.ElasticSearchJavaRestClient;
import org.apache.pulsar.io.elasticsearch.client.opensearch.OpenSearchHighLevelRestClient;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class RestClientFactoryTest {

    @Test
    @SneakyThrows
    public void testCompatibilityMode() {
        final ElasticSearchConfig config = new ElasticSearchConfig();
        config.setElasticSearchUrl("http://localhost:9200");

        config.setCompatibilityMode(ElasticSearchConfig.CompatibilityMode.ELASTICSEARCH_7);
        assertTrue(RestClientFactory.createClient(config, null) instanceof OpenSearchHighLevelRestClient);

        config.setCompatibilityMode(ElasticSearchConfig.CompatibilityMode.OPENSEARCH);
        assertTrue(RestClientFactory.createClient(config, null) instanceof OpenSearchHighLevelRestClient);

        config.setCompatibilityMode(ElasticSearchConfig.CompatibilityMode.ELASTICSEARCH);
        assertTrue(RestClientFactory.createClient(config, null) instanceof ElasticSearchJavaRestClient);

    }
}