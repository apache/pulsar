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

import static org.testng.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.pulsar.PulsarBrokerStarter;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

/**
 * @version $Revision$<br>
 *          Created on Sep 6, 2012
 */
public class PulsarBrokerStarterTest {
    /**
     * Tests the private static <code>loadConfig</code> method of {@link PulsarBrokerStarter} class: verifies (1) if the
     * method returns a non-null {@link ServiceConfiguration} instance where all required settings are filled in and (2)
     * if the property variables inside the given property file are correctly referred to that returned object.
     */
    @Test
    public void testLoadConfig() throws SecurityException, NoSuchMethodException, IOException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {

        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=z1.example.com,z2.example.com,z3.example.com");
        printWriter.println("globalZookeeperServers=gz1.example.com,gz2.example.com,gz3.example.com/foo");
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

        printWriter.close();
        testConfigFile.deleteOnExit();

        Method targetMethod = PulsarBrokerStarter.class.getDeclaredMethod("loadConfig", String.class);
        targetMethod.setAccessible(true);

        Object returnValue = targetMethod.invoke(PulsarBrokerStarter.class, testConfigFile.getAbsolutePath());

        Assert.assertTrue(ServiceConfiguration.class.isInstance(returnValue));
        ServiceConfiguration serviceConfig = (ServiceConfiguration) returnValue;
        Assert.assertEquals(serviceConfig.getZookeeperServers(), "z1.example.com,z2.example.com,z3.example.com");
        Assert.assertEquals(serviceConfig.getGlobalZookeeperServers(),
                "gz1.example.com,gz2.example.com,gz3.example.com/foo");
        Assert.assertFalse(serviceConfig.isBrokerDeleteInactiveTopicsEnabled());
        Assert.assertEquals(serviceConfig.getStatusFilePath(), "/tmp/status.html");
        Assert.assertEquals(serviceConfig.getBacklogQuotaDefaultLimitGB(), 18);
        Assert.assertEquals(serviceConfig.getClusterName(), "usc");
        Assert.assertEquals(serviceConfig.getBrokerClientAuthenticationPlugin(), "test.xyz.client.auth.plugin");
        Assert.assertEquals(serviceConfig.getBrokerClientAuthenticationParameters(), "role:my-role");
        Assert.assertEquals(serviceConfig.getSuperUserRoles(), Sets.newHashSet("appid1", "appid2"));
        Assert.assertEquals(serviceConfig.getManagedLedgerCursorRolloverTimeInSeconds(), 3000);
        Assert.assertEquals(serviceConfig.getManagedLedgerMaxEntriesPerLedger(), 25);
        Assert.assertEquals(serviceConfig.getManagedLedgerCursorMaxEntriesPerLedger(), 50);
        Assert.assertTrue(serviceConfig.isClientLibraryVersionCheckAllowUnversioned());
        Assert.assertTrue(serviceConfig.isClientLibraryVersionCheckEnabled());
        Assert.assertEquals(serviceConfig.getManagedLedgerMinLedgerRolloverTimeMinutes(), 34);
        Assert.assertEquals(serviceConfig.isBacklogQuotaCheckEnabled(), true);
        Assert.assertEquals(serviceConfig.getManagedLedgerDefaultMarkDeleteRateLimit(), 5.0);
        Assert.assertEquals(serviceConfig.getReplicationProducerQueueSize(), 50);
        Assert.assertEquals(serviceConfig.isReplicationMetricsEnabled(), false);
        Assert.assertEquals(serviceConfig.isBookkeeperClientHealthCheckEnabled(), true);
        Assert.assertEquals(serviceConfig.getBookkeeperClientHealthCheckErrorThresholdPerInterval(), 5);
        Assert.assertEquals(serviceConfig.isBookkeeperClientRackawarePolicyEnabled(), true);
        Assert.assertEquals(serviceConfig.getBookkeeperClientIsolationGroups(), "group1,group2");
        Assert.assertEquals(serviceConfig.getBookkeeperClientSpeculativeReadTimeoutInMillis(), 3000);
        Assert.assertEquals(serviceConfig.getBookkeeperClientTimeoutInSeconds(), 12345);
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
            Assert.assertTrue(e.getTargetException() instanceof IllegalArgumentException);
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

        Assert.assertTrue(ServiceConfiguration.class.isInstance(returnValue));
        ServiceConfiguration serviceConfig = (ServiceConfiguration) returnValue;
        Assert.assertEquals(serviceConfig.isLoadBalancerEnabled(), false);
        Assert.assertEquals(serviceConfig.getLoadBalancerHostUsageCheckIntervalMinutes(), 4);
        Assert.assertEquals(serviceConfig.getLoadBalancerReportUpdateThresholdPercentage(), 15);
        Assert.assertEquals(serviceConfig.getLoadBalancerReportUpdateMaxIntervalMinutes(), 20);
        Assert.assertEquals(serviceConfig.getLoadBalancerBrokerOverloadedThresholdPercentage(), 80);
        Assert.assertEquals(serviceConfig.getLoadBalancerBrokerUnderloadedThresholdPercentage(), 40);
        Assert.assertEquals(serviceConfig.getLoadBalancerSheddingIntervalMinutes(), 8);
        Assert.assertEquals(serviceConfig.getLoadBalancerSheddingGracePeriodMinutes(), 29);
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
        printWriter.println("globalZookeeperServers=");
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

        Assert.assertTrue(ServiceConfiguration.class.isInstance(returnValue));
        ServiceConfiguration serviceConfig = (ServiceConfiguration) returnValue;
        Assert.assertEquals(serviceConfig.getZookeeperServers(), "z1.example.com,z2.example.com,z3.example.com");
        Assert.assertEquals(serviceConfig.getGlobalZookeeperServers(), "z1.example.com,z2.example.com,z3.example.com");
        Assert.assertFalse(serviceConfig.isBrokerDeleteInactiveTopicsEnabled());
        Assert.assertEquals(serviceConfig.getStatusFilePath(), "/tmp/status.html");
        Assert.assertEquals(serviceConfig.getBacklogQuotaDefaultLimitGB(), 18);
        Assert.assertEquals(serviceConfig.getClusterName(), "usc");
        Assert.assertEquals(serviceConfig.getBrokerClientAuthenticationPlugin(), "test.xyz.client.auth.plugin");
        Assert.assertEquals(serviceConfig.getBrokerClientAuthenticationParameters(), "role:my-role");
        Assert.assertEquals(serviceConfig.getSuperUserRoles(), Sets.newHashSet("appid1", "appid2"));
        Assert.assertEquals(serviceConfig.getManagedLedgerCursorRolloverTimeInSeconds(), 3000);
        Assert.assertEquals(serviceConfig.getManagedLedgerMaxEntriesPerLedger(), 25);
        Assert.assertEquals(serviceConfig.getManagedLedgerCursorMaxEntriesPerLedger(), 50);
        Assert.assertTrue(serviceConfig.isClientLibraryVersionCheckAllowUnversioned());
        Assert.assertTrue(serviceConfig.isClientLibraryVersionCheckEnabled());
        Assert.assertEquals(serviceConfig.getReplicationConnectionsPerBroker(), 12);
    }

    /**
     * Verifies that the main throws {@link IllegalArgumentException} when no argument is given.
     */
    @Test
    public void testMainWithNoArgument() throws Exception {
        try {
            PulsarBrokerStarter.main(new String[0]);
            Assert.fail("No argument to main should've raised IllegalArgumentException!");
        } catch (IllegalArgumentException e) {
            // code should reach here.
        }
    }
}
