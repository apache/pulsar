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
package org.apache.pulsar.common.configuration;

import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.isComplete;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public class PulsarConfigurationLoaderTest {
    public class MockConfiguration implements PulsarConfiguration {
        private Properties properties = new Properties();
        
        private String zookeeperServers = "localhost:2181";
        private String globalZookeeperServers = "localhost:2184";
        private int brokerServicePort = 7650;
        private int brokerServicePortTls = 7651;
        private int webServicePort = 9080;
        private int webServicePortTls = 9443;
        private int notExistFieldInServiceConfig = 0;

        @Override
        public Properties getProperties() {
            return properties;
        }

        @Override
        public void setProperties(Properties properties) {
            this.properties = properties;
        }
    }

    @Test
    public void testConfigurationConverting() throws Exception {
        MockConfiguration mockConfiguration = new MockConfiguration();
        ServiceConfiguration serviceConfiguration = PulsarConfigurationLoader.convertFrom(mockConfiguration);

        // check whether converting correctly
        assertEquals(serviceConfiguration.getZookeeperServers(), "localhost:2181");
        assertEquals(serviceConfiguration.getGlobalZookeeperServers(), "localhost:2184");
        assertEquals(serviceConfiguration.getBrokerServicePort(), 7650);
        assertEquals(serviceConfiguration.getBrokerServicePortTls(), 7651);
        assertEquals(serviceConfiguration.getWebServicePort(), 9080);
        assertEquals(serviceConfiguration.getWebServicePortTls(), 9443);

        // check whether exception causes
        try {
            PulsarConfigurationLoader.convertFrom(mockConfiguration, false);
            fail();
        } catch (Exception e) {
            assertEquals(e.getClass(), IllegalArgumentException.class);
        }
    }

    @Test
    public void testPulsarConfiguraitonLoadingStream() throws Exception {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        final String zkServer = "z1.example.com,z2.example.com,z3.example.com";
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=" + zkServer);
        printWriter.println("globalZookeeperServers=gz1.example.com,gz2.example.com,gz3.example.com/foo");
        printWriter.println("brokerDeleteInactiveTopicsEnabled=true");
        printWriter.println("statusFilePath=/tmp/status.html");
        printWriter.println("managedLedgerDefaultEnsembleSize=1");
        printWriter.println("backlogQuotaDefaultLimitGB=18");
        printWriter.println("clusterName=usc");
        printWriter.println("brokerClientAuthenticationPlugin=test.xyz.client.auth.plugin");
        printWriter.println("brokerClientAuthenticationParameters=role:my-role");
        printWriter.println("superUserRoles=appid1,appid2");
        printWriter.println("brokerServicePort=7777");
        printWriter.println("managedLedgerDefaultMarkDeleteRateLimit=5.0");
        printWriter.close();
        testConfigFile.deleteOnExit();
        InputStream stream = new FileInputStream(testConfigFile);
        final ServiceConfiguration serviceConfig = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);
        assertNotNull(serviceConfig);
        assertEquals(serviceConfig.getZookeeperServers(), zkServer);
        assertEquals(serviceConfig.isBrokerDeleteInactiveTopicsEnabled(), true);
        assertEquals(serviceConfig.getBacklogQuotaDefaultLimitGB(), 18);
        assertEquals(serviceConfig.getClusterName(), "usc");
        assertEquals(serviceConfig.getBrokerClientAuthenticationParameters(), "role:my-role");
        assertEquals(serviceConfig.getBrokerServicePort(), 7777);
    }

    @Test
    public void testPulsarConfiguraitonLoadingSystemProperty() throws Exception {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        final String zkServer = "z1.example.com,z2.example.com,z3.example.com";
        final String globalZookeeperServers = "gz1.example.com,gz2.example.com,gz3.example.com/foo";
        final String clusterName = "usc";
        final int brokerServicePort = 7777;
        final boolean brokerDeleteInactiveTopicsEnabled = true;
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=$zkServer");
        printWriter.println("globalZookeeperServers=" + globalZookeeperServers);
        printWriter.println("brokerDeleteInactiveTopicsEnabled=${brokerDeleteInactiveTopicsEnabled}");
        printWriter.println("statusFilePath=$statusFilePath");
        printWriter.println("clusterName=${clusterName}");
        printWriter.println("brokerServicePort=$brokerServicePort");
        printWriter.close();
        testConfigFile.deleteOnExit();
        InputStream stream = new FileInputStream(testConfigFile);

        // set system-properties
        Map<String, String> propMap = Maps.newHashMap();
        propMap.put("zkServer", zkServer);
        propMap.put("clusterName", clusterName);
        propMap.put("brokerServicePort", Integer.toString(brokerServicePort));
        propMap.put("brokerDeleteInactiveTopicsEnabled", Boolean.toString(brokerDeleteInactiveTopicsEnabled));
        updateEnvironmentPropertyVariables(propMap);

        final ServiceConfiguration serviceConfig = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);
        assertNotNull(serviceConfig);
        assertEquals(serviceConfig.getZookeeperServers(), zkServer);
        assertEquals(serviceConfig.getGlobalZookeeperServers(), globalZookeeperServers);
        assertEquals(serviceConfig.isBrokerDeleteInactiveTopicsEnabled(), brokerDeleteInactiveTopicsEnabled);
        assertEquals(serviceConfig.getClusterName(), clusterName);
        assertEquals(serviceConfig.getBrokerServicePort(), brokerServicePort);
        assertEquals(serviceConfig.getStatusFilePath(), "$statusFilePath");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static void updateEnvironmentPropertyVariables(Map<String, String> newProperties) throws Exception {
        try {
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            Field envVarFields = processEnvironmentClass.getDeclaredField("theEnvironment");
            envVarFields.setAccessible(true);
            Map<String, String> env = (Map<String, String>) envVarFields.get(null);
            env.putAll(newProperties);
            Field caseInsensitiveEnvFields = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
            caseInsensitiveEnvFields.setAccessible(true);
            Map<String, String> caseInsensitiveEnvVars = (Map<String, String>) caseInsensitiveEnvFields.get(null);
            caseInsensitiveEnvVars.putAll(newProperties);
        } catch (NoSuchFieldException e) {
            Class[] classes = Collections.class.getDeclaredClasses();
            Map<String, String> env = System.getenv();
            for (Class clazz : classes) {
                if ("java.util.Collections$UnmodifiableMap".equals(clazz.getName())) {
                    Field field = clazz.getDeclaredField("m");
                    field.setAccessible(true);
                    Object obj = field.get(env);
                    Map<String, String> map = (Map<String, String>) obj;
                    map.clear();
                    map.putAll(newProperties);
                }
            }
        }
    }

    @Test
    public void testPulsarConfiguraitonLoadingProp() throws Exception {
        final String zk = "localhost:2184";
        final Properties prop = new Properties();
        prop.setProperty("zookeeperServers", zk);
        final ServiceConfiguration serviceConfig = PulsarConfigurationLoader.create(prop, ServiceConfiguration.class);
        assertNotNull(serviceConfig);
        assertEquals(serviceConfig.getZookeeperServers(), zk);
    }

    @Test
    public void testPulsarConfiguraitonComplete() throws Exception {
        final String zk = "localhost:2184";
        final Properties prop = new Properties();
        prop.setProperty("zookeeperServers", zk);
        final ServiceConfiguration serviceConfig = PulsarConfigurationLoader.create(prop, ServiceConfiguration.class);
        try {
            isComplete(serviceConfig);
            fail("it should fail as config is not complete");
        } catch (IllegalArgumentException e) {
            // Ok
        }
    }

    @Test
    public void testComplete() throws Exception {
        TestCompleteObject complete = this.new TestCompleteObject();
        assertTrue(isComplete(complete));
    }

    @Test
    public void testInComplete() throws IllegalAccessException {

        try {
            isComplete(this.new TestInCompleteObjectRequired());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            isComplete(this.new TestInCompleteObjectMin());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            isComplete(this.new TestInCompleteObjectMax());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            isComplete(this.new TestInCompleteObjectMix());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }
    }

    class TestCompleteObject {
        @FieldContext(required = true)
        String required = "I am not null";
        @FieldContext(required = false)
        String optional;
        @FieldContext
        String optional2;
        @FieldContext(minValue = 1)
        int minValue = 2;
        @FieldContext(minValue = 1, maxValue = 3)
        int minMaxValue = 2;

    }

    class TestInCompleteObjectRequired {
        @FieldContext(required = true)
        String inValidRequired;
    }

    class TestInCompleteObjectMin {
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMin = 0;
    }

    class TestInCompleteObjectMax {
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMax = 4;
    }

    class TestInCompleteObjectMix {
        @FieldContext(required = true)
        String inValidRequired;
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMin = 0;
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMax = 4;
    }
}
