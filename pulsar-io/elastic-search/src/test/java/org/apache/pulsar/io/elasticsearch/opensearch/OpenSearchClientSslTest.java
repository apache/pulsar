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
package org.apache.pulsar.io.elasticsearch.opensearch;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.elasticsearch.ElasticSearchClient;
import org.apache.pulsar.io.elasticsearch.ElasticSearchConfig;
import org.apache.pulsar.io.elasticsearch.ElasticSearchSslConfig;
import org.apache.pulsar.io.elasticsearch.ElasticSearchTestBase;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.Test;

/*https://opensearch.org/docs/latest/opensearch/install/docker-security/*/
public class OpenSearchClientSslTest extends ElasticSearchTestBase {

    static final String INDEX = "myindex";

    static final String SSL_RESOURCE_DIR = MountableFile.forClasspathResource("ssl").getFilesystemPath();
    static final  String CONFIG_DIR = "/usr/share/opensearch/config";

    public OpenSearchClientSslTest() {
        super(OPENSEARCH);
    }

    private static Map<String, String> sslEnv() {
        Map<String, String> map = new HashMap<>();
        map.put("plugins.security.disabled", "false");
        map.put("plugins.security.ssl.http.enabled", "true");

        map.put("plugins.security.ssl.http.enabled", "true");
        map.put("plugins.security.ssl.http.pemkey_filepath", CONFIG_DIR + "/ssl/elasticsearch.pem");
        map.put("plugins.security.ssl.http.pemcert_filepath", CONFIG_DIR + "/ssl/elasticsearch.crt");
        map.put("plugins.security.ssl.http.pemtrustedcas_filepath", CONFIG_DIR + "/ssl/cacert.pem");
        map.put("plugins.security.ssl.transport.enabled", "true");
        map.put("plugins.security.ssl.transport.pemkey_filepath", CONFIG_DIR + "/ssl/elasticsearch.pem");
        map.put("plugins.security.ssl.transport.pemcert_filepath", CONFIG_DIR + "/ssl/elasticsearch.crt");
        map.put("plugins.security.ssl.transport.pemtrustedcas_filepath", CONFIG_DIR + "/ssl/cacert.pem");
        return map;
    }

    @Test
    public void testSslBasic() throws IOException {
        try (ElasticsearchContainer container = createElasticsearchContainer()
                .withFileSystemBind(SSL_RESOURCE_DIR, CONFIG_DIR + "/ssl")
                .withEnv(sslEnv())
                .waitingFor(Wait.forLogMessage(".*Node started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)))) {
            container.start();

            ElasticSearchConfig config = new ElasticSearchConfig()
                    .setElasticSearchUrl("https://" + container.getHttpHostAddress())
                    .setIndexName(INDEX)
                    .setUsername("admin")
                    .setPassword("0pEn7earch!")
                    .setSsl(new ElasticSearchSslConfig()
                            .setEnabled(true)
                            .setTruststorePath(SSL_RESOURCE_DIR + "/truststore.jks")
                            .setTruststorePassword("changeit"));
            ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));
            testIndexExists(client);
        }
    }

    @Test
    public void testSslWithHostnameVerification() throws IOException {
        try (ElasticsearchContainer container = createElasticsearchContainer()
                .withFileSystemBind(SSL_RESOURCE_DIR, CONFIG_DIR + "/ssl")
                .withEnv(sslEnv())
                .withEnv("plugins.security.ssl.transport.enforce_hostname_verification", "true")
                .waitingFor(Wait.forLogMessage(".*Node started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)))) {
            container.start();

            ElasticSearchConfig config = new ElasticSearchConfig()
                    .setElasticSearchUrl("https://" + container.getHttpHostAddress())
                    .setIndexName(INDEX)
                    .setUsername("admin")
                    .setPassword("0pEn7earch!")
                    .setSsl(new ElasticSearchSslConfig()
                            .setEnabled(true)
                            .setProtocols("TLSv1.2")
                            .setHostnameVerification(true)
                            .setTruststorePath(SSL_RESOURCE_DIR + "/truststore.jks")
                            .setTruststorePassword("changeit"));
            ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));
            testIndexExists(client);
        }
    }

    @Test
    public void testSslWithClientAuth() throws IOException {
        try (ElasticsearchContainer container = createElasticsearchContainer()
                .withFileSystemBind(SSL_RESOURCE_DIR, CONFIG_DIR + "/ssl")
                .withEnv(sslEnv())
                .waitingFor(Wait.forLogMessage(".*Node started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)))) {
            container.start();

            ElasticSearchConfig config = new ElasticSearchConfig()
                    .setElasticSearchUrl("https://" + container.getHttpHostAddress())
                    .setIndexName(INDEX)
                    .setUsername("admin")
                    .setPassword("0pEn7earch!")
                    .setSsl(new ElasticSearchSslConfig()
                            .setEnabled(true)
                            .setHostnameVerification(true)
                            .setTruststorePath(SSL_RESOURCE_DIR + "/truststore.jks")
                            .setTruststorePassword("changeit")
                            .setKeystorePath(SSL_RESOURCE_DIR + "/keystore.jks")
                            .setKeystorePassword("changeit"));
            ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));
            testIndexExists(client);
        }
    }


    public void testIndexExists(ElasticSearchClient client) throws IOException {
        assertFalse(client.indexExists("mynewindex"));
        assertTrue(client.createIndexIfNeeded("mynewindex"));
        assertTrue(client.indexExists("mynewindex"));
        assertFalse(client.createIndexIfNeeded("mynewindex"));
    }

}
