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

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Arrays;
import lombok.Cleanup;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.PulsarBrokerStarter.BrokerStarter;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.metadata.bookkeeper.BookieStatsProviders;
import org.apache.pulsar.metrics.prometheus.bookkeeper.PrometheusMetricsProvider;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.annotations.Test;
import picocli.CommandLine.Option;

@Test(groups = "broker")
public class PulsarBrokerStarterTest {

    private File createValidBrokerConfigFile() throws FileNotFoundException {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("metadataStoreUrl=zk:z1.example.com,z2.example.com,z3.example.com");
        printWriter.println("configurationMetadataStoreUrl=zk:gz1.example.com,gz2.example.com,gz3.example.com/foo");
        printWriter.println("brokerDeleteInactiveTopicsEnabled=false");
        printWriter.println("statusFilePath=/tmp/status.html");
        printWriter.println("managedLedgerDefaultEnsembleSize=1");
        printWriter.println("managedLedgerDefaultWriteQuorum=1");
        printWriter.println("managedLedgerDefaultAckQuorum=1");
        printWriter.println("managedLedgerMaxEntriesPerLedger=25");
        printWriter.println("managedLedgerCursorMaxEntriesPerLedger=50");
        printWriter.println("managedLedgerCursorRolloverTimeInSeconds=3000");
        printWriter.println("bookkeeperClientHealthCheckEnabled=true");
        printWriter.println("bookkeeperClientHealthCheckErrorThresholdPerInterval=5");
        printWriter.println("bookkeeperClientRackawarePolicyEnabled=true");
        printWriter.println("bookkeeperClientRegionawarePolicyEnabled=false");
        printWriter.println("bookkeeperClientMinNumRacksPerWriteQuorum=5");
        printWriter.println("bookkeeperClientEnforceMinNumRacksPerWriteQuorum=true");
        printWriter.println("bookkeeperClientReorderReadSequenceEnabled=false");
        printWriter.println("bookkeeperClientIsolationGroups=group1,group2");
        printWriter.println("backlogQuotaDefaultLimitGB=18");
        printWriter.println("clusterName=usc");
        printWriter.println("brokerClientAuthenticationPlugin=test.xyz.client.auth.plugin");
        printWriter.println("brokerClientAuthenticationParameters=role:my-role");
        printWriter.println("superUserRoles=appid1,appid2");
        printWriter.println("clientLibraryVersionCheckEnabled=true");
        printWriter.println("clientLibraryVersionCheckAllowUnversioned=true");
        printWriter.println("managedLedgerMinLedgerRolloverTimeMinutes=34");
        printWriter.println("managedLedgerMaxLedgerRolloverTimeMinutes=34");
        printWriter.println("brokerServicePort=7777");
        printWriter.println("managedLedgerDefaultMarkDeleteRateLimit=5.0");
        printWriter.println("replicationProducerQueueSize=50");
        printWriter.println("bookkeeperClientTimeoutInSeconds=12345");
        printWriter.println("bookkeeperClientSpeculativeReadTimeoutInMillis=3000");
        printWriter.println("enableRunBookieTogether=true");
        printWriter.println("bookkeeperExplicitLacIntervalInMills=5");

        printWriter.close();
        testConfigFile.deleteOnExit();
        return testConfigFile;
    }

    /**
     * Tests the private static <code>loadConfig</code> method of {@link PulsarBrokerStarter} class: verifies (1) if the
     * method returns a non-null {@link ServiceConfiguration} instance where all required settings are filled in and (2)
     * if the property variables inside the given property file are correctly referred to that returned object.
     */
    @SuppressWarnings("deprecation")
    public void testLoadConfig() throws SecurityException, NoSuchMethodException, IOException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {

        File testConfigFile = createValidBrokerConfigFile();
        Method targetMethod = PulsarBrokerStarter.class.getDeclaredMethod("loadConfig", String.class);
        targetMethod.setAccessible(true);

        Object returnValue = targetMethod.invoke(PulsarBrokerStarter.class, testConfigFile.getAbsolutePath());

        assertTrue(returnValue instanceof ServiceConfiguration);
        ServiceConfiguration serviceConfig = (ServiceConfiguration) returnValue;
        assertEquals(serviceConfig.getMetadataStoreUrl(), "zk:z1.example.com,z2.example.com,z3.example.com");
        assertEquals(serviceConfig.getConfigurationMetadataStoreUrl(), "zk:gz1.example.com,gz2.example.com,gz3.example"
                + ".com/foo");
        assertFalse(serviceConfig.isBrokerDeleteInactiveTopicsEnabled());
        assertEquals(serviceConfig.getStatusFilePath(), "/tmp/status.html");
        assertEquals(serviceConfig.getBacklogQuotaDefaultLimitGB(), 18);
        assertEquals(serviceConfig.getClusterName(), "usc");
        assertEquals(serviceConfig.getBrokerClientAuthenticationPlugin(), "test.xyz.client.auth.plugin");
        assertEquals(serviceConfig.getBrokerClientAuthenticationParameters(), "role:my-role");
        assertEquals(serviceConfig.getSuperUserRoles(), Sets.newHashSet("appid1", "appid2"));
        assertEquals(serviceConfig.getManagedLedgerCursorRolloverTimeInSeconds(), 3000);
        assertEquals(serviceConfig.getManagedLedgerMaxEntriesPerLedger(), 25);
        assertEquals(serviceConfig.getManagedLedgerCursorMaxEntriesPerLedger(), 50);
        assertTrue(serviceConfig.isClientLibraryVersionCheckEnabled());
        assertEquals(serviceConfig.getManagedLedgerMinLedgerRolloverTimeMinutes(), 34);
        assertTrue(serviceConfig.isBacklogQuotaCheckEnabled());
        assertEquals(serviceConfig.getManagedLedgerDefaultMarkDeleteRateLimit(), 5.0);
        assertEquals(serviceConfig.getReplicationProducerQueueSize(), 50);
        assertTrue(serviceConfig.isReplicationMetricsEnabled());
        assertTrue(serviceConfig.isBookkeeperClientHealthCheckEnabled());
        assertEquals(serviceConfig.getBookkeeperClientHealthCheckErrorThresholdPerInterval(), 5);
        assertTrue(serviceConfig.isBookkeeperClientRackawarePolicyEnabled());
        assertEquals(serviceConfig.getBookkeeperClientMinNumRacksPerWriteQuorum(), 5);
        assertTrue(serviceConfig.isBookkeeperClientEnforceMinNumRacksPerWriteQuorum());
        assertFalse(serviceConfig.isBookkeeperClientRegionawarePolicyEnabled());
        assertFalse(serviceConfig.isBookkeeperClientReorderReadSequenceEnabled());
        assertEquals(serviceConfig.getBookkeeperClientIsolationGroups(), "group1,group2");
        assertEquals(serviceConfig.getBookkeeperClientSpeculativeReadTimeoutInMillis(), 3000);
        assertEquals(serviceConfig.getBookkeeperClientTimeoutInSeconds(), 12345);
        assertEquals(serviceConfig.getBookkeeperExplicitLacIntervalInMills(), 5);
    }

    @Test
    public void testLoadConfigWithException() throws Exception {

        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("brokerDeleteInactiveTopicsEnabled=false");
        printWriter.println("statusFilePath=/tmp/pulsar_broker/status.html");
        printWriter.println("managedLedgerDefaultEnsembleSize01");
        printWriter.println("managedLedgerDefaultWriteQuorum=0");
        printWriter.println("managedLedgerDefaultAckQuorum=0");

        printWriter.close();
        testConfigFile.deleteOnExit();

        try {
            Method targetMethod = PulsarBrokerStarter.class.getDeclaredMethod("loadConfig", String.class);
            targetMethod.setAccessible(true);
            targetMethod.invoke(PulsarBrokerStarter.class, testConfigFile.getAbsolutePath());
            fail("Should fail w/ illegal argument exception");
        } catch (InvocationTargetException e) {
            // OK, expected
            assertTrue(e.getTargetException() instanceof IllegalArgumentException);
        }
    }

    /**
     * Tests the private static <code>loadConfig</code> method of {@link PulsarBrokerStarter} class: verifies (1) if the
     * method returns a non-null {@link ServiceConfiguration} instance where all required settings are filled in and (2)
     * if the property variables inside the given property file are correctly referred to that returned object.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testLoadBalancerConfig() throws SecurityException, NoSuchMethodException, IOException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }

        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("metadataStoreUrl=zk:z1.example.com,z2.example.com,z3.example.com");
        printWriter.println("statusFilePath=/usr/share/pulsar_broker/status.html");
        printWriter.println("clusterName=test");
        printWriter.println("managedLedgerDefaultEnsembleSize=1");
        printWriter.println("managedLedgerDefaultWriteQuorum=1");
        printWriter.println("managedLedgerDefaultAckQuorum=1");

        printWriter.println("loadBalancerEnabled=false");
        printWriter.println("loadBalancerHostUsageCheckIntervalMinutes=4");
        printWriter.println("loadBalancerReportUpdateThresholdPercentage=15");
        printWriter.println("loadBalancerReportUpdateMaxIntervalMinutes=20");
        printWriter.println("loadBalancerBrokerOverloadedThresholdPercentage=80");
        printWriter.println("loadBalancerBrokerUnderloadedThresholdPercentage=40");
        printWriter.println("loadBalancerSheddingIntervalMinutes=8");
        printWriter.println("loadBalancerSheddingGracePeriodMinutes=29");
        printWriter.close();
        testConfigFile.deleteOnExit();

        Method targetMethod = PulsarBrokerStarter.class.getDeclaredMethod("loadConfig", String.class);
        targetMethod.setAccessible(true);
        Object returnValue = targetMethod.invoke(PulsarBrokerStarter.class, testConfigFile.getAbsolutePath());

        assertTrue(returnValue instanceof ServiceConfiguration);
        ServiceConfiguration serviceConfig = (ServiceConfiguration) returnValue;
        assertFalse(serviceConfig.isLoadBalancerEnabled());
        assertEquals(serviceConfig.getLoadBalancerHostUsageCheckIntervalMinutes(), 4);
        assertEquals(serviceConfig.getLoadBalancerReportUpdateThresholdPercentage(), 15);
        assertEquals(serviceConfig.getLoadBalancerReportUpdateMaxIntervalMinutes(), 20);
        assertEquals(serviceConfig.getLoadBalancerBrokerOverloadedThresholdPercentage(), 80);
        assertEquals(serviceConfig.getLoadBalancerBrokerUnderloadedThresholdPercentage(), 40);
        assertEquals(serviceConfig.getLoadBalancerSheddingIntervalMinutes(), 8);
        assertEquals(serviceConfig.getLoadBalancerSheddingGracePeriodMinutes(), 29);
    }

    /**
     * Tests the private static <code>loadConfig</code> method of {@link PulsarBrokerStarter} class: verifies (1) if the
     * method returns a non-null {@link ServiceConfiguration} instance where all required settings are filled in and (2)
     * if the property variables inside the given property file are correctly referred to that returned object.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testGlobalZooKeeperConfig() throws SecurityException, NoSuchMethodException, IOException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("metadataStoreUrl=zk:z1.example.com,z2.example.com,z3.example.com");
        printWriter.println("configurationMetadataStoreUrl=");
        printWriter.println("brokerDeleteInactiveTopicsEnabled=false");
        printWriter.println("statusFilePath=/tmp/status.html");
        printWriter.println("managedLedgerDefaultEnsembleSize=1");
        printWriter.println("managedLedgerDefaultWriteQuorum=1");
        printWriter.println("managedLedgerDefaultAckQuorum=1");
        printWriter.println("managedLedgerMaxEntriesPerLedger=25");
        printWriter.println("managedLedgerCursorMaxEntriesPerLedger=50");
        printWriter.println("managedLedgerCursorRolloverTimeInSeconds=3000");
        printWriter.println("backlogQuotaDefaultLimitGB=18");
        printWriter.println("clusterName=usc");
        printWriter.println("brokerClientAuthenticationPlugin=test.xyz.client.auth.plugin");
        printWriter.println("brokerClientAuthenticationParameters=role:my-role");
        printWriter.println("superUserRoles=appid1,appid2");
        printWriter.println("pulsar.broker.enableClientVersionCheck=true");
        printWriter.println("pulsar.broker.allowUnversionedClients=true");
        printWriter.println("clientLibraryVersionCheckEnabled=true");
        printWriter.println("clientLibraryVersionCheckAllowUnversioned=true");
        printWriter.println("replicationConnectionsPerBroker=12");

        printWriter.close();
        testConfigFile.deleteOnExit();

        Method targetMethod = PulsarBrokerStarter.class.getDeclaredMethod("loadConfig", String.class);
        targetMethod.setAccessible(true);

        Object returnValue = targetMethod.invoke(PulsarBrokerStarter.class, testConfigFile.getAbsolutePath());

        assertTrue(returnValue instanceof ServiceConfiguration);
        ServiceConfiguration serviceConfig = (ServiceConfiguration) returnValue;
        assertEquals(serviceConfig.getMetadataStoreUrl(), "zk:z1.example.com,z2.example.com,z3.example.com");
        assertEquals(serviceConfig.getConfigurationMetadataStoreUrl(), "zk:z1.example.com,z2.example.com,z3.example"
                + ".com");
        assertFalse(serviceConfig.isBrokerDeleteInactiveTopicsEnabled());
        assertEquals(serviceConfig.getStatusFilePath(), "/tmp/status.html");
        assertEquals(serviceConfig.getBacklogQuotaDefaultLimitGB(), 18);
        assertEquals(serviceConfig.getClusterName(), "usc");
        assertEquals(serviceConfig.getBrokerClientAuthenticationPlugin(), "test.xyz.client.auth.plugin");
        assertEquals(serviceConfig.getBrokerClientAuthenticationParameters(), "role:my-role");
        assertEquals(serviceConfig.getSuperUserRoles(), Sets.newHashSet("appid1", "appid2"));
        assertEquals(serviceConfig.getManagedLedgerCursorRolloverTimeInSeconds(), 3000);
        assertEquals(serviceConfig.getManagedLedgerMaxEntriesPerLedger(), 25);
        assertEquals(serviceConfig.getManagedLedgerCursorMaxEntriesPerLedger(), 50);
        assertTrue(serviceConfig.isClientLibraryVersionCheckEnabled());
        assertEquals(serviceConfig.getReplicationConnectionsPerBroker(), 12);
    }

    /**
     * Verifies that the main throws {@link FileNotFoundException} when no argument is given.
     */
    @Test
    public void testMainWithNoArgument() throws Exception {
        @Cleanup("shutdown")
        BrokerStarter brokerStarter = new BrokerStarter();
        @Cleanup
        StringWriter err = new StringWriter();
        @Cleanup
        PrintWriter printWriter = new PrintWriter(err);
        brokerStarter.getCommander().setErr(printWriter);
        assertEquals(brokerStarter.start(new String[0]), 1);
        assertTrue(err.toString().contains("FileNotFoundException"));
    }

    /**
     * Verifies that the main throws {@link IllegalArgumentException}
     * when no config file for bookie and bookie auto recovery is given.
     */
    @Test
    public void testMainRunBookieAndAutoRecoveryNoConfig() throws Exception {
        File testConfigFile = createValidBrokerConfigFile();
        String[] args = {"-c", testConfigFile.getAbsolutePath(), "-rb", "-ra", "-bc", ""};
        @Cleanup("shutdown")
        BrokerStarter starter = new BrokerStarter();
        @Cleanup
        StringWriter err = new StringWriter();
        @Cleanup
        PrintWriter printWriter = new PrintWriter(err);
        starter.getCommander().setErr(printWriter);
        assertEquals(starter.start(args), 1);
        assertTrue(err.toString().contains("No configuration file for Bookie"));
    }

    /**
     * Verifies that the main throws {@link IllegalArgumentException}
     * when no config file for bookie auto recovery is given.
     */
    @Test
    public void testMainRunBookieRecoveryNoConfig() throws Exception {
        File testConfigFile = createValidBrokerConfigFile();
        String[] args = {"-c", testConfigFile.getAbsolutePath(), "-ra", "-bc", ""};
        @Cleanup("shutdown")
        BrokerStarter starter = new BrokerStarter();
        @Cleanup
        StringWriter err = new StringWriter();
        @Cleanup
        PrintWriter printWriter = new PrintWriter(err);
        starter.getCommander().setErr(printWriter);
        assertEquals(starter.start(args), 1);
        assertTrue(err.toString().contains("No configuration file for Bookie"));
    }

    /**
     * Verifies that the main throws {@link IllegalArgumentException} when no config file for bookie is given.
     */
    @Test
    public void testMainRunBookieNoConfig() throws Exception {
        File testConfigFile = createValidBrokerConfigFile();
        String[] args = {"-c", testConfigFile.getAbsolutePath(), "-rb", "-bc", ""};
        @Cleanup("shutdown")
        BrokerStarter starter = new BrokerStarter();
        @Cleanup
        StringWriter err = new StringWriter();
        @Cleanup
        PrintWriter printWriter = new PrintWriter(err);
        starter.getCommander().setErr(printWriter);
        assertEquals(starter.start(args), 1);
        assertTrue(err.toString().contains("No configuration file for Bookie"));
    }

    /**
     * Verifies that the main throws {@link IllegalArgumentException} when no config file for bookie .
     */
    @Test
    public void testMainEnableRunBookieThroughBrokerConfig() throws Exception {
        File testConfigFile = createValidBrokerConfigFile();
        String[] args = {"-c", testConfigFile.getAbsolutePath()};
        @Cleanup("shutdown")
        BrokerStarter starter = new BrokerStarter();
        @Cleanup
        StringWriter err = new StringWriter();
        @Cleanup
        PrintWriter printWriter = new PrintWriter(err);
        starter.getCommander().setErr(printWriter);
        assertEquals(starter.start(args), 1);
        assertTrue(err.toString().contains("IllegalArgumentException"));
    }

    @Test
    public void testMainGenerateDocs() throws Exception {
        PrintStream oldStream = System.out;
        try {
            ByteArrayOutputStream baoStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baoStream));

            Class<?> argumentsClass = Class.forName("org.apache.pulsar.PulsarBrokerStarter$StarterArguments");
            PulsarBrokerStarter.main(new String[]{"-g"});

            String message = baoStream.toString();

            Field[] fields = argumentsClass.getDeclaredFields();
            for (Field field : fields) {
                boolean fieldHasAnno = field.isAnnotationPresent(Option.class);
                if (fieldHasAnno) {
                    Option fieldAnno = field.getAnnotation(Option.class);
                    String[] names = fieldAnno.names();
                    String nameStr = Arrays.asList(names).toString();
                    nameStr = nameStr.substring(1, nameStr.length() - 1);
                    assertTrue(message.indexOf(nameStr) > 0);
                }
            }
        } finally {
            System.setOut(oldStream);
        }
    }

    /**
     * Starts a bookie the way {@code -rb} does, against an embedded ZooKeeper, and checks that the broker's
     * registry never sees it. {@code EmbeddedServer} builds its own provider from the configuration when none is
     * passed in, and that one lands on the 1.x default registry.
     */
    @Test
    public void testInProcessBookieStatsProviderStaysOutOfTheBrokerScrape() throws Exception {
        LocalBookkeeperEnsemble zk = new LocalBookkeeperEnsemble(0, 0);
        zk.start();
        File bookieDir = Files.createTempDirectory("bk-starter-test").toFile();
        StatsProvider provider = null;
        LifecycleComponentStack bookieServer = null;
        try {
            ServerConfiguration bookieConf = new ServerConfiguration();
            // An explicit id keeps the cookie validation from resolving the host name of the local address.
            bookieConf.setBookieId("bk-starter-test");
            bookieConf.setStatsProviderClass(PrometheusMetricsProvider.class);
            bookieConf.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
            bookieConf.setProperty("metadataServiceUri", "zk://127.0.0.1:" + zk.getZookeeperPort() + "/ledgers");
            bookieConf.setJournalDirName(bookieDir.getPath());
            bookieConf.setLedgerDirNames(new String[] {bookieDir.getPath()});
            bookieConf.setLedgerStorageClass(DbLedgerStorage.class.getName());
            bookieConf.setProperty("dbStorage_writeCacheMaxSizeMb", 2);
            bookieConf.setProperty("dbStorage_readAheadCacheMaxSizeMb", 1);
            bookieConf.setProperty("dbStorage_rocksDB_writeBufferSizeMB", 1);
            bookieConf.setProperty("dbStorage_rocksDB_blockCacheSize", 1024 * 1024);
            bookieConf.setJournalSyncData(false);
            bookieConf.setAllowLoopback(true);
            bookieConf.setBookiePort(0);
            bookieConf.setAllowEphemeralPorts(true);
            bookieConf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);

            provider = BookieStatsProviders.create(bookieConf);
            provider.start(bookieConf);
            bookieServer = BookieStatsProviders.buildBookieServer(bookieConf, provider);
            bookieServer.start();

            // The bookie's own stats endpoint serves the JVM metrics and the bookie's metrics.
            StringWriter bookieEndpoint = new StringWriter();
            provider.writeAllMetrics(bookieEndpoint);
            assertThat(bookieEndpoint.toString())
                    .contains("# TYPE jvm_memory_direct_bytes_used gauge\n")
                    .contains("bookie_SERVER_STATUS");
            // But none of them reached the registry the broker's /metrics scrapes.
            assertThat(PrometheusRegistry.defaultRegistry.scrape(name -> name.startsWith("jvm_")
                    || name.startsWith("process_") || name.startsWith("bookie_")).size()).isZero();
        } finally {
            if (bookieServer != null) {
                bookieServer.close();
            }
            if (provider != null) {
                provider.stop();
            }
            zk.stop();
            FileUtils.deleteDirectory(bookieDir);
        }
        // Any other provider still comes from its no-arg constructor.
        ServerConfiguration nullProviderConf = new ServerConfiguration();
        nullProviderConf.setStatsProviderClass(NullStatsProvider.class);
        assertThat(BookieStatsProviders.create(nullProviderConf)).isInstanceOf(NullStatsProvider.class);
    }
}
