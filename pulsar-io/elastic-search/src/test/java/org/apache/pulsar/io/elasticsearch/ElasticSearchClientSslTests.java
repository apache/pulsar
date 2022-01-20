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

import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// see https://www.elastic.co/guide/en/elasticsearch/reference/current/security-settings.html#ssl-tls-settings
public class ElasticSearchClientSslTests {

    public static final String ELASTICSEARCH_IMAGE = Optional.ofNullable(System.getenv("ELASTICSEARCH_IMAGE"))
            .orElse("docker.elastic.co/elasticsearch/elasticsearch:7.10.2-amd64");

    final static String INDEX = "myindex";

    final static String sslResourceDir = MountableFile.forClasspathResource("ssl").getFilesystemPath();
    final static  String configDir = "/usr/share/elasticsearch/config";

    @Test
    public void testSslBasic() throws IOException {
        try(ElasticsearchContainer container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                .withCreateContainerCmdModifier(c -> c.withName("elasticsearch"))
                .withFileSystemBind(sslResourceDir, configDir + "/ssl")
                .withEnv("ELASTIC_PASSWORD","elastic")  // boostrap password
                .withEnv("xpack.license.self_generated.type", "trial")
                .withEnv("xpack.security.enabled", "true")
                .withEnv("xpack.security.http.ssl.enabled", "true")
                .withEnv("xpack.security.http.ssl.client_authentication", "optional")
                .withEnv("xpack.security.http.ssl.key", configDir + "/ssl/elasticsearch.key")
                .withEnv("xpack.security.http.ssl.certificate", configDir + "/ssl/elasticsearch.crt")
                .withEnv("xpack.security.http.ssl.certificate_authorities", configDir + "/ssl/cacert.crt")
                .withEnv("xpack.security.transport.ssl.enabled", "true")
                .withEnv("xpack.security.transport.ssl.verification_mode", "certificate")
                .withEnv("xpack.security.transport.ssl.key", configDir + "/ssl/elasticsearch.key")
                .withEnv("xpack.security.transport.ssl.certificate", configDir + "/ssl/elasticsearch.crt")
                .withEnv("xpack.security.transport.ssl.certificate_authorities", configDir + "/ssl/cacert.crt")
                .waitingFor(Wait.forLogMessage(".*(Security is enabled|Active license).*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)))) {
            container.start();

            ElasticSearchConfig config = new ElasticSearchConfig()
                    .setElasticSearchUrl("https://" + container.getHttpHostAddress())
                    .setIndexName(INDEX)
                    .setUsername("elastic")
                    .setPassword("elastic")
                    .setSsl(new ElasticSearchSslConfig()
                            .setEnabled(true)
                            .setTruststorePath(sslResourceDir + "/truststore.jks")
                            .setTruststorePassword("changeit"));
            ElasticSearchClient client = new ElasticSearchClient(config);
            testIndexExists(client);
        }
    }

    @Test
    public void testSslWithHostnameVerification() throws IOException {
        try(ElasticsearchContainer container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                .withCreateContainerCmdModifier(c -> c.withName("elasticsearch"))
                .withFileSystemBind(sslResourceDir, configDir + "/ssl")
                .withEnv("ELASTIC_PASSWORD","elastic")  // boostrap password
                .withEnv("xpack.license.self_generated.type", "trial")
                .withEnv("xpack.security.enabled", "true")
                .withEnv("xpack.security.http.ssl.enabled", "true")
                .withEnv("xpack.security.http.ssl.supported_protocols", "TLSv1.2,TLSv1.1")
                .withEnv("xpack.security.http.ssl.client_authentication", "optional")
                .withEnv("xpack.security.http.ssl.key", configDir + "/ssl/elasticsearch.key")
                .withEnv("xpack.security.http.ssl.certificate", configDir + "/ssl/elasticsearch.crt")
                .withEnv("xpack.security.http.ssl.certificate_authorities", configDir + "/ssl/cacert.crt")
                .withEnv("xpack.security.transport.ssl.enabled", "true")
                .withEnv("xpack.security.transport.ssl.verification_mode", "full")
                .withEnv("xpack.security.transport.ssl.key", configDir + "/ssl/elasticsearch.key")
                .withEnv("xpack.security.transport.ssl.certificate", configDir + "/ssl/elasticsearch.crt")
                .withEnv("xpack.security.transport.ssl.certificate_authorities", configDir + "/ssl/cacert.crt")
                .waitingFor(Wait.forLogMessage(".*(Security is enabled|Active license).*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)))) {
            container.start();

            ElasticSearchConfig config = new ElasticSearchConfig()
                    .setElasticSearchUrl("https://" + container.getHttpHostAddress())
                    .setIndexName(INDEX)
                    .setUsername("elastic")
                    .setPassword("elastic")
                    .setSsl(new ElasticSearchSslConfig()
                            .setEnabled(true)
                            .setProtocols("TLSv1.2")
                            .setHostnameVerification(true)
                            .setTruststorePath(sslResourceDir + "/truststore.jks")
                            .setTruststorePassword("changeit"));
            ElasticSearchClient client = new ElasticSearchClient(config);
            testIndexExists(client);
        }
    }

    @Test
    public void testSslWithClientAuth() throws IOException {
        try(ElasticsearchContainer container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                .withCreateContainerCmdModifier(c -> c.withName("elasticsearch"))
                .withFileSystemBind(sslResourceDir, configDir + "/ssl")
                .withEnv("ELASTIC_PASSWORD","elastic")  // boostrap password
                .withEnv("xpack.license.self_generated.type", "trial")
                .withEnv("xpack.security.enabled", "true")
                .withEnv("xpack.security.http.ssl.enabled", "true")
                .withEnv("xpack.security.http.ssl.client_authentication", "required")
                .withEnv("xpack.security.http.ssl.key", configDir + "/ssl/elasticsearch.key")
                .withEnv("xpack.security.http.ssl.certificate", configDir + "/ssl/elasticsearch.crt")
                .withEnv("xpack.security.http.ssl.certificate_authorities", configDir + "/ssl/cacert.crt")
                .withEnv("xpack.security.transport.ssl.enabled", "true")
                .withEnv("xpack.security.transport.ssl.verification_mode", "full")
                .withEnv("xpack.security.transport.ssl.key", configDir + "/ssl/elasticsearch.key")
                .withEnv("xpack.security.transport.ssl.certificate", configDir + "/ssl/elasticsearch.crt")
                .withEnv("xpack.security.transport.ssl.certificate_authorities", configDir + "/ssl/cacert.crt")
                .waitingFor(Wait.forLogMessage(".*(Security is enabled|Active license).*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)))) {
            container.start();

            ElasticSearchConfig config = new ElasticSearchConfig()
                    .setElasticSearchUrl("https://" + container.getHttpHostAddress())
                    .setIndexName(INDEX)
                    .setUsername("elastic")
                    .setPassword("elastic")
                    .setSsl(new ElasticSearchSslConfig()
                            .setEnabled(true)
                            .setHostnameVerification(true)
                            .setTruststorePath(sslResourceDir + "/truststore.jks")
                            .setTruststorePassword("changeit")
                            .setKeystorePath(sslResourceDir + "/keystore.jks")
                            .setKeystorePassword("changeit"));
            ElasticSearchClient client = new ElasticSearchClient(config);
            testIndexExists(client);
        }
    }


    private void testIndexExists(ElasticSearchClient client) throws IOException {
        assertFalse(client.indexExists("mynewindex"));
        assertTrue(client.createIndexIfNeeded("mynewindex"));
        assertTrue(client.indexExists("mynewindex"));
        assertFalse(client.createIndexIfNeeded("mynewindex"));
    }

}
