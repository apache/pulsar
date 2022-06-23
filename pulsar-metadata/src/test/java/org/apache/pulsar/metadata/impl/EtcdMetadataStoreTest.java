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
package org.apache.pulsar.metadata.impl;

import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.launcher.EtcdClusterFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.Test;

@Slf4j
public class EtcdMetadataStoreTest {

    @Test
    public void testTlsInstance() throws Exception {
        @Cleanup
        EtcdCluster etcdCluster = EtcdClusterFactory.buildCluster("test-tls", 1, true);
        etcdCluster.start();

        EtcdConfig etcdConfig = EtcdConfig.builder().useTls(true)
                .tlsProvider(null)
                .authority("etcd0")
                .tlsTrustCertsFilePath(Resources.getResource("ssl/cert/ca.pem").getPath())
                .tlsKeyFilePath(Resources.getResource("ssl/cert/client-key-pk8.pem").getPath())
                .tlsCertificateFilePath(Resources.getResource("ssl/cert/client.pem").getPath())
                .build();
        Path etcdConfigPath = Files.createTempFile("etcd_config", ".yml");
        new ObjectMapper(new YAMLFactory()).writeValue(etcdConfigPath.toFile(), etcdConfig);

        String metadataURL =
                "etcd:" + etcdCluster.getClientEndpoints().stream().map(URI::toString).collect(Collectors.joining(","));

        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(metadataURL,
                MetadataStoreConfig.builder().configFilePath(etcdConfigPath.toString()).build());

        store.put("/test", "value".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();

        assertTrue(store.exists("/test").join());
    }
}