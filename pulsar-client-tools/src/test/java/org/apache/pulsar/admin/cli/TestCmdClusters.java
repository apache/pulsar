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
package org.apache.pulsar.admin.cli;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.assertj.core.util.Maps;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCmdClusters {

    private PulsarAdmin pulsarAdmin;

    private CmdClusters cmdClusters;

    private Clusters clusters;

    @BeforeMethod
    public void setup() throws Exception {
        pulsarAdmin = mock(PulsarAdmin.class);
        clusters = mock(Clusters.class);
        when(pulsarAdmin.clusters()).thenReturn(clusters);

        cmdClusters = spy(new CmdClusters(() -> pulsarAdmin));
    }

    @Test
    public void testCmdClusterConfigFile() throws Exception {
        ClusterData clusterData = buildClusterData();
        testCmdClusterConfigFile(clusterData, clusterData);
    }

    private void testCmdClusterConfigFile(ClusterData testClusterData, ClusterData expectedClusterData)
            throws Exception {
        File file = Files.createTempFile("tmp_cluster", ".yaml").toFile();
        ObjectMapperFactory.getYamlMapper().writer().writeValue(file, testClusterData);
        Assert.assertEquals(testClusterData, CmdUtils.loadConfig(file.getAbsolutePath(), ClusterData.class));

        // test create cluster
        cmdClusters.run(new String[]{"create", "test_cluster", "--cluster-config-file", file.getAbsolutePath()});
        verify(clusters).createCluster(eq("test_cluster"), eq(expectedClusterData));

        cmdClusters.run(new String[]{"update", "test_cluster", "--cluster-config-file", file.getAbsolutePath()});
        verify(clusters).updateCluster(eq("test_cluster"), eq(expectedClusterData));
    }

    public ClusterData buildClusterData() {
        return ClusterData.builder()
                .serviceUrlTls("https://my-service.url:4443")
                .authenticationPlugin("authenticationPlugin")
                .authenticationParameters("authenticationParameters")
                .proxyProtocol(ProxyProtocol.SNI)
                .brokerClientTlsEnabled(true)
                .brokerClientTlsEnabledWithKeyStore(true)
                .brokerClientTlsTrustStoreType("JKS")
                .brokerClientTlsTrustStore("/var/private/tls/client.truststore.jks")
                .brokerClientTlsTrustStorePassword("clientpw")
                .build();
    }

    @Test
    public void testListCmd() throws Exception {
        List<String> clusterList = Lists.newArrayList("us-west", "us-east", "us-cent");
        List<String> clusterResultList = Lists.newArrayList("us-west", "us-east", "us-cent(*)");
        Map<String, String> configurations = Maps.newHashMap("clusterName", "us-cent");

        Clusters clusters = mock(Clusters.class);
        Brokers brokers = mock(Brokers.class);
        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.clusters()).thenReturn(clusters);
        when(admin.brokers()).thenReturn(brokers);
        doReturn(clusterList).when(clusters).getClusters();
        doReturn(configurations).when(brokers).getRuntimeConfigurations();

        CmdClusters cmd = new CmdClusters(() -> admin);

        @Cleanup
        StringWriter stringWriter = new StringWriter();
        @Cleanup
        PrintWriter printWriter = new PrintWriter(stringWriter);
        cmd.getCommander().setOut(printWriter);

        cmd.run("list".split("\\s+"));
        Assert.assertEquals(stringWriter.toString(), String.join("\n", clusterList) + "\n");

        @Cleanup
        StringWriter stringWriter1 = new StringWriter();
        @Cleanup
        PrintWriter printWriter1 = new PrintWriter(stringWriter1);
        cmd.getCommander().setOut(printWriter1);
        cmd.run("list -c".split("\\s+"));
        Assert.assertEquals(stringWriter1.toString(), String.join("\n", clusterResultList) + "\n");
    }

    @Test
    public void testGetClusterMigration() throws Exception {
        cmdClusters.run(new String[]{"get-cluster-migration", "test_cluster"});
        verify(clusters, times(1)).getClusterMigration("test_cluster");
    }
}
