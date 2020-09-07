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
package org.apache.pulsar;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

/**
 * @version $Revision$<br>
 *          Created on Sep 6, 2012
 */
public class PulsarBrokerStarterTest {

    private File createValidBrokerConfigFile() throws FileNotFoundException {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=z1.example.com,z2.example.com,z3.example.com");
        printWriter.println("configurationStoreServers=gz1.example.com,gz2.example.com,gz3.example.com/foo");
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
    @Test
    public void testLoadConfig() throws SecurityException, NoSuchMethodException, IOException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {

        File testConfigFile = createValidBrokerConfigFile();
        Method targetMethod = PulsarBrokerStarter.class.getDeclaredMethod("loadConfig", String.class);
        targetMethod.setAccessible(true);

        Object returnValue = targetMethod.invoke(PulsarBrokerStarter.class, testConfigFile.getAbsolutePath());

        assertTrue(returnValue instanceof ServiceConfiguration);
        ServiceConfiguration serviceConfig = (ServiceConfiguration) returnValue;
        assertEquals(serviceConfig.getZookeeperServers(), "z1.example.com,z2.example.com,z3.example.com");
        assertEquals(serviceConfig.getConfigurationStoreServers(), "gz1.example.com,gz2.example.com,gz3.example.com/foo");
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
        assertFalse(serviceConfig.isReplicationMetricsEnabled());
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
    @Test
    public void testLoadBalancerConfig() throws SecurityException, NoSuchMethodException, IOException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }

        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=z1.example.com,z2.example.com,z3.example.com");
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
    @Test
    public void testGlobalZooKeeperConfig() throws SecurityException, NoSuchMethodException, IOException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=z1.example.com,z2.example.com,z3.example.com");
        printWriter.println("configurationStoreServers=");
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
        assertEquals(serviceConfig.getZookeeperServers(), "z1.example.com,z2.example.com,z3.example.com");
        assertEquals(serviceConfig.getConfigurationStoreServers(), "z1.example.com,z2.example.com,z3.example.com");
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
        try {
            PulsarBrokerStarter.main(new String[0]);
            fail("No argument to main should've raised FileNotFoundException for no broker config!");
        } catch (FileNotFoundException e) {
            // code should reach here.
        }
    }

    /**
     * Verifies that the main throws {@link IllegalArgumentException}
     * when no config file for bookie and bookie auto recovery is given.
     */
    @Test
    public void testMainRunBookieAndAutoRecoveryNoConfig() throws Exception {
        try {
            File testConfigFile = createValidBrokerConfigFile();
            String[] args = {"-c", testConfigFile.getAbsolutePath(), "-rb", "-ra", "-bc", ""};
            PulsarBrokerStarter.main(args);
            fail("No Config file for bookie auto recovery should've raised IllegalArgumentException!");
        } catch (IllegalArgumentException e) {
            // code should reach here.
            e.printStackTrace();
            assertEquals(e.getMessage(), "No configuration file for Bookie");
        }
    }

    /**
     * Verifies that the main throws {@link IllegalArgumentException}
     * when no config file for bookie auto recovery is given.
     */
    @Test
    public void testMainRunBookieRecoveryNoConfig() throws Exception {
        try {
            File testConfigFile = createValidBrokerConfigFile();
            String[] args = {"-c", testConfigFile.getAbsolutePath(), "-ra", "-bc", ""};
            PulsarBrokerStarter.main(args);
            fail("No Config file for bookie auto recovery should've raised IllegalArgumentException!");
        } catch (IllegalArgumentException e) {
            // code should reach here.
            assertEquals(e.getMessage(), "No configuration file for Bookie");
        }
    }

    /**
     * Verifies that the main throws {@link IllegalArgumentException} when no config file for bookie is given.
     */
    @Test
    public void testMainRunBookieNoConfig() throws Exception {
        try {
            File testConfigFile = createValidBrokerConfigFile();
            String[] args = {"-c", testConfigFile.getAbsolutePath(), "-rb", "-bc", ""};
            PulsarBrokerStarter.main(args);
            fail("No Config file for bookie should've raised IllegalArgumentException!");
        } catch (IllegalArgumentException e) {
            // code should reach here
            assertEquals(e.getMessage(), "No configuration file for Bookie");
        }
    }

    /**
     * Verifies that the main throws {@link IllegalArgumentException} when no config file for bookie .
     */
    @Test
    public void testMainEnableRunBookieThroughBrokerConfig() throws Exception {
        try {
            File testConfigFile = createValidBrokerConfigFile();
            String[] args = {"-c", testConfigFile.getAbsolutePath()};
            PulsarBrokerStarter.main(args);
            fail("No argument to main should've raised IllegalArgumentException for no bookie config!");
        } catch (IllegalArgumentException e) {
            // code should reach here.
        }
    }
}
