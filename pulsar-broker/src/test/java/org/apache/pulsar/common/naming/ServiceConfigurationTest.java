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
package org.apache.pulsar.common.naming;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.testng.annotations.Test;

@Test(groups = "broker-naming")
public class ServiceConfigurationTest {

    final String fileName = "configurations/pulsar_broker_test.conf"; // test-resource file

    /**
     * test {@link ServiceConfiguration} initialization
     *
     * @throws Exception
     */
    @Test
    public void testInit() throws Exception {
        final String zookeeperServer = "localhost:2184";
        final int brokerServicePort = 1000;
        InputStream newStream = updateProp(zookeeperServer, String.valueOf(brokerServicePort), "ns1,ns2", 0.05);
        final ServiceConfiguration config = PulsarConfigurationLoader.create(newStream, ServiceConfiguration.class);
        assertTrue(isNotBlank(config.getMetadataStoreUrl()));
        assertTrue(config.getBrokerServicePort().isPresent()
                && config.getBrokerServicePort().get().equals(brokerServicePort));
        assertEquals(config.getBootstrapNamespaces().get(1), "ns2");
        assertEquals(config.getBrokerDeleteInactiveTopicsMode(), InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        assertEquals(config.getDefaultNamespaceBundleSplitAlgorithm(), "topic_count_equally_divide");
        assertEquals(config.getSupportedNamespaceBundleSplitAlgorithms().size(), 1);
        assertEquals(config.getMaxMessagePublishBufferSizeInMB(), -1);
        assertEquals(config.getManagedLedgerDataReadPriority(), "bookkeeper-first");
        assertEquals(config.getBacklogQuotaDefaultLimitGB(), 0.05);
    }

    @Test
    public void testOptionalSettingEmpty() throws Exception {
        String confFile = "loadBalancerOverrideBrokerNicSpeedGbps=\n";
        InputStream stream = new ByteArrayInputStream(confFile.getBytes());
        final ServiceConfiguration config = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);
        assertEquals(config.getLoadBalancerOverrideBrokerNicSpeedGbps(), Optional.empty());
    }

    @Test
    public void testOptionalSettingPresent() throws Exception {
        String confFile = "loadBalancerOverrideBrokerNicSpeedGbps=5\n";
        InputStream stream = new ByteArrayInputStream(confFile.getBytes());
        final ServiceConfiguration config = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);
        assertEquals(config.getLoadBalancerOverrideBrokerNicSpeedGbps(), Optional.of(5.0));
    }

    @Test
    public void testServicePortsEmpty() throws Exception {
        String confFile = "brokerServicePort=\nwebServicePort=\n";
        InputStream stream = new ByteArrayInputStream(confFile.getBytes());
        final ServiceConfiguration config = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);
        assertEquals(config.getBrokerServicePort(), Optional.empty());
        assertEquals(config.getWebServicePort(), Optional.empty());
    }

    /**
     * test {@link ServiceConfiguration} with incorrect values.
     *
     * @throws Exception
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInitFailure() throws Exception {
        final String zookeeperServer = "localhost:2184";
        InputStream newStream = updateProp(zookeeperServer, "invalid-string", null, 0.005);
        PulsarConfigurationLoader.create(newStream, ServiceConfiguration.class);
    }

    private InputStream updateProp(String zookeeperServer, String brokerServicePort, String namespace, double backlogQuotaGB)
            throws IOException {
        checkNotNull(fileName);
        Properties properties = new Properties();
        InputStream stream = this.getClass().getClassLoader().getResourceAsStream(fileName);
        properties.load(stream);
        properties.setProperty("zookeeperServers", zookeeperServer);
        properties.setProperty("brokerServicePort", brokerServicePort);
        properties.setProperty("backlogQuotaDefaultLimitGB", "" + backlogQuotaGB);
        if (namespace != null)
            properties.setProperty("bootstrapNamespaces", namespace);
        StringWriter writer = new StringWriter();
        properties.list(new PrintWriter(writer));
        return new ByteArrayInputStream(writer.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testZookeeperServers() throws Exception {
        String confFile = "zookeeperServers=zk1:2181\n";
        @Cleanup
        InputStream stream = new ByteArrayInputStream(confFile.getBytes());
        final ServiceConfiguration conf = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);

        assertEquals(conf.getMetadataStoreUrl(), "zk1:2181");
        assertEquals(conf.getConfigurationMetadataStoreUrl(), "zk1:2181");
        assertEquals(conf.getBookkeeperMetadataStoreUrl(), "metadata-store:zk1:2181");
        assertFalse(conf.isConfigurationStoreSeparated());
        assertFalse(conf.isBookkeeperMetadataStoreSeparated());
    }

    @Test
    public void testMetadataStoreUrl() throws Exception {
        String confFile = "metadataStoreUrl=zk1:2181\n";
        @Cleanup
        InputStream stream = new ByteArrayInputStream(confFile.getBytes());
        final ServiceConfiguration conf = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);

        assertEquals(conf.getMetadataStoreUrl(), "zk1:2181");
        assertEquals(conf.getConfigurationMetadataStoreUrl(), "zk1:2181");
        assertEquals(conf.getBookkeeperMetadataStoreUrl(), "metadata-store:zk1:2181");
        assertFalse(conf.isConfigurationStoreSeparated());
        assertFalse(conf.isBookkeeperMetadataStoreSeparated());
    }


    @Test
    public void testGlobalZookeeper() throws Exception {
        String confFile = "metadataStoreUrl=zk1:2181\n" +
                "globalZookeeperServers=zk2:2182\n"
                ;
        @Cleanup
        InputStream stream = new ByteArrayInputStream(confFile.getBytes());
        final ServiceConfiguration conf = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);

        assertEquals(conf.getMetadataStoreUrl(), "zk1:2181");
        assertEquals(conf.getConfigurationMetadataStoreUrl(), "zk2:2182");
        assertEquals(conf.getBookkeeperMetadataStoreUrl(), "metadata-store:zk1:2181");
        assertTrue(conf.isConfigurationStoreSeparated());
        assertFalse(conf.isBookkeeperMetadataStoreSeparated());
    }

    @Test
    public void testConfigurationStore() throws Exception {
        String confFile = "metadataStoreUrl=zk1:2181\n" +
                "configurationStoreServers=zk2:2182\n"
                ;
        @Cleanup
        InputStream stream = new ByteArrayInputStream(confFile.getBytes());
        final ServiceConfiguration conf = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);

        assertEquals(conf.getMetadataStoreUrl(), "zk1:2181");
        assertEquals(conf.getConfigurationMetadataStoreUrl(), "zk2:2182");
        assertEquals(conf.getBookkeeperMetadataStoreUrl(), "metadata-store:zk1:2181");
        assertTrue(conf.isConfigurationStoreSeparated());
        assertFalse(conf.isBookkeeperMetadataStoreSeparated());
    }

    @Test
    public void testConfigurationMetadataStoreUrl() throws Exception {
        String confFile = "metadataStoreUrl=zk1:2181\n" +
                "configurationMetadataStoreUrl=zk2:2182\n"
                ;
        @Cleanup
        InputStream stream = new ByteArrayInputStream(confFile.getBytes());
        final ServiceConfiguration conf = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);

        assertEquals(conf.getMetadataStoreUrl(), "zk1:2181");
        assertEquals(conf.getConfigurationMetadataStoreUrl(), "zk2:2182");
        assertEquals(conf.getBookkeeperMetadataStoreUrl(), "metadata-store:zk1:2181");
        assertTrue(conf.isConfigurationStoreSeparated());
        assertFalse(conf.isBookkeeperMetadataStoreSeparated());
    }

    @Test
    public void testBookkeeperMetadataStore() throws Exception {
        String confFile = "metadataStoreUrl=zk1:2181\n" +
                "configurationMetadataStoreUrl=zk2:2182\n" +
                "bookkeeperMetadataServiceUri=xx:other-system\n";
        @Cleanup
        InputStream stream = new ByteArrayInputStream(confFile.getBytes());
        final ServiceConfiguration conf = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);

        assertEquals(conf.getMetadataStoreUrl(), "zk1:2181");
        assertEquals(conf.getConfigurationMetadataStoreUrl(), "zk2:2182");
        assertEquals(conf.getBookkeeperMetadataStoreUrl(), "xx:other-system");
        assertTrue(conf.isConfigurationStoreSeparated());
        assertTrue(conf.isBookkeeperMetadataStoreSeparated());
    }

    @Test
    public void testConfigFileDefaults() throws Exception {
        try (FileInputStream stream = new FileInputStream("../conf/broker.conf")) {
            final ServiceConfiguration javaConfig = PulsarConfigurationLoader.create(new Properties(), ServiceConfiguration.class);
            final ServiceConfiguration fileConfig = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);
            List<String> toSkip = Arrays.asList("properties", "class");
            int counter = 0;
            for (PropertyDescriptor pd : Introspector.getBeanInfo(ServiceConfiguration.class).getPropertyDescriptors()) {
                if (pd.getReadMethod() == null || toSkip.contains(pd.getName())) {
                    continue;
                }
                final String key = pd.getName();
                final Object javaValue = pd.getReadMethod().invoke(javaConfig);
                final Object fileValue = pd.getReadMethod().invoke(fileConfig);
                assertTrue(Objects.equals(javaValue, fileValue), "property '"
                        + key + "' conf/broker.conf default value doesn't match java default value\nConf: "+ fileValue + "\nJava: " + javaValue);
                counter++;
            }
            assertEquals(counter, 378);
        }

    }
}
