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

import java.util.Optional;
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
}
