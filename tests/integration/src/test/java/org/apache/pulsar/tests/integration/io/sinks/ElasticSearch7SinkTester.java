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

import java.util.Optional;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class ElasticSearch7SinkTester extends ElasticSearchSinkTester {

    public static final String ELASTICSEARCH_7 = Optional.ofNullable(System.getenv("ELASTICSEARCH_IMAGE_V7"))
            .orElse("docker.elastic.co/elasticsearch/elasticsearch:7.17.7");


    public ElasticSearch7SinkTester(boolean schemaEnable) {
        super(schemaEnable);
    }

    @Override
    protected ElasticsearchContainer createSinkService(PulsarCluster cluster) {
        return new ElasticsearchContainer(ELASTICSEARCH_7)
                .withEnv("ES_JAVA_OPTS", "-Xms128m -Xmx256m");
    }

}
