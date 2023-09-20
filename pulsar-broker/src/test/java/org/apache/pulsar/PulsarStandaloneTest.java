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
package org.apache.pulsar;

import static org.apache.commons.io.FileUtils.cleanDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.List;
import lombok.Cleanup;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarStandaloneTest {

    @DataProvider
    public Object[][] enableBrokerClientAuth() {
        return new Object[][] { { true }, { false } };
    }

    @Test
    public void testStandaloneWithRocksDB() throws Exception {
        String[] args = new String[]{"--config",
                "./src/test/resources/configurations/pulsar_broker_test_standalone_with_rocksdb.conf"};
        final int bookieNum = 3;
        final File tempDir = IOUtils.createTempDir("standalone", "test");

        PulsarStandaloneStarter standalone = new PulsarStandaloneStarter(args);
        standalone.setBkDir(tempDir.getAbsolutePath());
        standalone.setNumOfBk(bookieNum);

        standalone.startBookieWithMetadataStore();
        List<ServerConfiguration> firstBsConfs = standalone.bkCluster.getBsConfs();
        Assert.assertEquals(firstBsConfs.size(), bookieNum);
        standalone.close();

        // start twice, read cookie from local folder
        standalone.startBookieWithMetadataStore();
        List<ServerConfiguration> secondBsConfs = standalone.bkCluster.getBsConfs();
        Assert.assertEquals(secondBsConfs.size(), bookieNum);

        for (int i = 0; i < bookieNum; i++) {
            ServerConfiguration conf1 = firstBsConfs.get(i);
            ServerConfiguration conf2 = secondBsConfs.get(i);
            Assert.assertEquals(conf1.getBookiePort(), conf2.getBookiePort());
        }
        standalone.close();
        cleanDirectory(tempDir);
    }

    @Test(dataProvider = "enableBrokerClientAuth")
    public void testMetadataInitialization(boolean enableBrokerClientAuth) throws Exception {
        final File metadataDir = IOUtils.createTempDir("standalone", "metadata");
        @Cleanup final PulsarStandaloneStarter standalone = new PulsarStandaloneStarter(new String[]{
                "--config",
                "./src/test/resources/configurations/standalone_no_client_auth.conf",
                "-nss",
                "-nfw",
                "--metadata-url",
                "rocksdb://" + metadataDir.getAbsolutePath()
        });
        if (enableBrokerClientAuth) {
            standalone.getConfig().setBrokerClientAuthenticationPlugin(
                    MockTokenAuthenticationProvider.MockAuthentication.class.getName());
        }
        final File bkDir = IOUtils.createTempDir("standalone", "bk");
        standalone.setNumOfBk(1);
        standalone.setBkDir(bkDir.getAbsolutePath());
        standalone.start();

        @Cleanup PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:8080")
                .authentication(new MockTokenAuthenticationProvider.MockAuthentication())
                .build();
        if (enableBrokerClientAuth) {
            assertTrue(admin.clusters().getClusters().contains("test_cluster"));
            assertTrue(admin.tenants().getTenants().contains("public"));
            assertTrue(admin.namespaces().getNamespaces("public").contains("public/default"));
        } else {
            assertTrue(admin.clusters().getClusters().isEmpty());
            admin.clusters().createCluster("test_cluster", ClusterData.builder()
                    .serviceUrl("http://localhost:8080/")
                    .brokerServiceUrl("pulsar://localhost:6650/")
                    .build());
            assertTrue(admin.tenants().getTenants().isEmpty());
            admin.tenants().createTenant("public", TenantInfo.builder()
                    .adminRoles(Collections.singleton("admin"))
                    .allowedClusters(Collections.singleton("test_cluster"))
                    .build());

            assertTrue(admin.namespaces().getNamespaces("public").isEmpty());
            admin.namespaces().createNamespace("public/default");
        }

        String topic = "test-get-topic-bundle-range";
        admin.topics().createNonPartitionedTopic(topic);
        assertEquals(admin.lookups().getBundleRange(topic), "0xc0000000_0xffffffff");

        standalone.close();
        cleanDirectory(bkDir);
        cleanDirectory(metadataDir);
    }
}
