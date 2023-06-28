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
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

@Slf4j
public class ElasticSearchTestBase {

    private static final String ELASTICSEARCH_IMAGE = Optional.ofNullable(System.getenv("ELASTICSEARCH_IMAGE"))
            .orElse("docker.elastic.co/elasticsearch/elasticsearch:7.16.3-amd64");

    protected static ElasticsearchContainer createElasticsearchContainer() {
        return new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                .withEnv("ES_JAVA_OPTS", "-Xms128m -Xmx256m")
                .withEnv("ingest.geoip.downloader.enabled", "false")
                .withLogConsumer(o -> log.info("elastic> {}", o.getUtf8String()));


    }

}
