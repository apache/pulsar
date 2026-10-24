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
package org.apache.pulsar.testclient;

import static org.apache.pulsar.client.api.ProxyProtocol.SNI;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import picocli.CommandLine;

public class PerformanceBaseArgumentsTest {

    @Test
    public void testReadFromConfigFile() throws Exception {
        final PerformanceBaseArguments args = new PerformanceBaseArguments("") {
            @Override
            public void run() throws Exception {

            }
        };

        String confFile = "./src/test/resources/perf_client1.conf";
        Properties prop = new Properties(System.getProperties());
        try (FileInputStream fis = new FileInputStream(confFile)) {
            prop.load(fis);
        }
        args.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
        args.parse(new String[]{});


        assertEquals(args.serviceURL, "https://my-pulsar:8443/");
        assertEquals(args.authPluginClassName,
                "org.apache.pulsar.testclient.PerfClientUtilsTest.MyAuth");
        assertEquals(args.authParams, "myparams");
        assertEquals(args.tlsTrustCertsFilePath, "./path");
        assertTrue(args.tlsAllowInsecureConnection);
        assertTrue(args.tlsHostnameVerificationEnable);
        assertEquals(args.proxyServiceURL, "https://my-proxy-pulsar:4443/");
        assertEquals(args.proxyProtocol, SNI);
    }

    @Test
    public void testReadFromConfigFileWithoutProxyUrl() {


        final PerformanceBaseArguments args = new PerformanceBaseArguments("") {
            @Override
            public void run() throws Exception {

            }

        };
        String confFile = "./src/test/resources/performance_client2.conf";

        File tempConfigFile = new File(confFile);
        if (tempConfigFile.exists()) {
            tempConfigFile.delete();
        }
        try {
            Properties props = new Properties();

            Map<String, String> configs = Map.of("brokerServiceUrl", "https://my-pulsar:8443/",
                    "authPlugin", "org.apache.pulsar.testclient.PerfClientUtilsTest.MyAuth",
                    "authParams", "myparams",
                    "tlsTrustCertsFilePath", "./path",
                    "tlsAllowInsecureConnection", "true",
                    "tlsEnableHostnameVerification", "true"
            );
            props.putAll(configs);
            FileOutputStream out = new FileOutputStream(tempConfigFile);
            props.store(out, "properties file");
            out.close();
            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(confFile)) {
                prop.load(fis);
            }
            args.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            args.parse(new String[]{});

            assertEquals(args.serviceURL, "https://my-pulsar:8443/");
            assertEquals(args.authPluginClassName,
                    "org.apache.pulsar.testclient.PerfClientUtilsTest.MyAuth");
            assertEquals(args.authParams, "myparams");
            assertEquals(args.tlsTrustCertsFilePath, "./path");
            assertTrue(args.tlsAllowInsecureConnection);
            assertTrue(args.tlsHostnameVerificationEnable);

        } catch (IOException e) {
            e.printStackTrace();
            fail("Error while updating/reading config file");
        } finally {
            tempConfigFile.delete();
        }
    }

    @Test
    public void testReadFromConfigFileProxyProtocolException() {

        AtomicBoolean calledVar2 = new AtomicBoolean();

        final PerformanceBaseArguments args = new PerformanceBaseArguments("") {
            @Override
            public void run() throws Exception {

            }
        };
        String confFile = "./src/test/resources/performance_client3.conf";
        File tempConfigFile = new File(confFile);
        if (tempConfigFile.exists()) {
            tempConfigFile.delete();
        }
        try {
            Properties props = new Properties();

            Map<String, String> configs = Map.of("brokerServiceUrl", "https://my-pulsar:8443/",
                    "authPlugin", "org.apache.pulsar.testclient.PerfClientUtilsTest.MyAuth",
                    "authParams", "myparams",
                    "tlsTrustCertsFilePath", "./path",
                    "tlsAllowInsecureConnection", "true",
                    "tlsEnableHostnameVerification", "true",
                    "proxyServiceURL", "https://my-proxy-pulsar:4443/",
                    "proxyProtocol", "TEST"
            );
            props.putAll(configs);
            FileOutputStream out = new FileOutputStream(tempConfigFile);
            props.store(out, "properties file");
            out.close();

            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(confFile)) {
                prop.load(fis);
            }
            args.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            try {
                args.parse(new String[]{});
            } catch (CommandLine.ParameterException e){
                calledVar2.set(true);
            }
            assertTrue(calledVar2.get());
        } catch (IOException e) {
            e.printStackTrace();
            fail("Error while updating/reading config file");
        } finally {
            tempConfigFile.delete();
        }
    }

    @DataProvider(name = "memoryLimitCliArgumentProvider")
    public Object[][] memoryLimitCliArgumentProvider() {
        return new Object[][]{
                {new String[]{"-ml", "1"}, 1L},
                {new String[]{"-ml", "1K"}, 1024L},
                {new String[]{"--memory-limit", "1G"}, 1024 * 1024 * 1024}
        };
    }

    @Test(dataProvider = "memoryLimitCliArgumentProvider")
    public void testMemoryLimitCliArgument(String[] cliArgs, long expectedMemoryLimit) throws Exception {
        for (String cmd : List.of(
                "pulsar-perf read",
                "pulsar-perf produce",
                "pulsar-perf consume",
                "pulsar-perf transaction"
        )) {
            // Arrange
            final PerformanceBaseArguments baseArgument = new PerformanceBaseArguments("") {
                @Override
                public void run() throws Exception {

                }

            };
            String confFile = "./src/test/resources/perf_client1.conf";
            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(confFile)) {
                prop.load(fis);
            }
            baseArgument.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            baseArgument.parse(new String[]{});

            // Act
            baseArgument.parseCLI();
            baseArgument.getCommander().execute(cliArgs);

            // Assert
            assertEquals(baseArgument.memoryLimit, expectedMemoryLimit);
        }
    }

    @DataProvider(name = "invalidMemoryLimitCliArgumentProvider")
    public Object[][] invalidMemoryLimitCliArgumentProvider() {
        return new Object[][]{
                {new String[]{"-ml", "-1"}},
                {new String[]{"-ml", "1C"}},
                {new String[]{"--memory-limit", "1Q"}}
        };
    }

    @Test
    public void testMemoryLimitCliArgumentDefault() throws Exception {
        for (String cmd : List.of(
                "pulsar-perf read",
                "pulsar-perf produce",
                "pulsar-perf consume",
                "pulsar-perf transaction"
        )) {
            // Arrange
            final PerformanceBaseArguments baseArgument = new PerformanceBaseArguments("") {
                @Override
                public void run() throws Exception {

                }

            };
            String confFile = "./src/test/resources/perf_client1.conf";
            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(confFile)) {
                prop.load(fis);
            }
            baseArgument.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            baseArgument.parse(new String[]{});

            // Act
            baseArgument.parseCLI();

            // Assert
            assertEquals(baseArgument.memoryLimit, PerformanceBaseArguments.DEFAULT_MEMORY_LIMIT_BYTES);
            assertEquals(baseArgument.memoryLimit, (long) (0.5d * DirectMemoryUtils.jvmMaxDirectMemory()));
        }
    }

    @Test
    public void testMemoryLimitCanBeDisabled() throws Exception {
        for (String cmd : List.of(
                "pulsar-perf read",
                "pulsar-perf produce",
                "pulsar-perf consume",
                "pulsar-perf transaction"
        )) {
            // Arrange
            final PerformanceBaseArguments baseArgument = new PerformanceBaseArguments("") {
                @Override
                public void run() throws Exception {

                }

            };
            String confFile = "./src/test/resources/perf_client1.conf";
            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(confFile)) {
                prop.load(fis);
            }
            baseArgument.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            baseArgument.parse(new String[]{"-ml", "0"});

            // Act
            baseArgument.parseCLI();

            // Assert
            assertEquals(baseArgument.memoryLimit, 0L);
        }
    }

    @Test
    public void testReadConfigFileWithUseKeyStoreTlsWithoutTrustStorePath() throws Exception {
        final PerformanceBaseArguments args = new PerformanceBaseArguments("") {
            @Override
            public void run() throws Exception {

            }
        };

        String confFile = "./perf_client_keystore_invalid.conf";

        File tempConfigFile = new File(confFile);
        if (tempConfigFile.exists()) {
            tempConfigFile.delete();
        }
        try {
            Properties props = new Properties();

            Map<String, String> configs = Map.of("brokerServiceUrl", "https://my-pulsar:8443/",
                    "authPlugin", "org.apache.pulsar.testclient.PerfClientUtilsTest.MyAuth",
                    "authParams", "myparams",
                    "useKeyStoreTls", "true"
            );
            props.putAll(configs);
            FileOutputStream out = new FileOutputStream(tempConfigFile);
            props.store(out, "properties file");
            out.close();
            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(confFile)) {
                prop.load(fis);
            }
            args.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            args.parse(new String[]{});
            // No truststore is fine: the client falls back to the platform trust store,
            // or trusts every certificate with --tls-allow-insecure.
            args.validate();
            assertTrue(args.useKeyStoreTls);
            assertEquals(args.tlsTrustStorePath, "");
        } catch (IOException e) {
            fail("Error while updating/reading config file");
        } finally {
            tempConfigFile.delete();
        }
    }

    @Test
    public void testReadConfigFileWithUseKeyStoreTlsAndTrustStorePath() throws Exception {
        final PerformanceBaseArguments args = new PerformanceBaseArguments("") {
            @Override
            public void run() throws Exception {

            }
        };

        String confFile = "./perf_client_keystore.conf";

        File tempConfigFile = new File(confFile);
        if (tempConfigFile.exists()) {
            tempConfigFile.delete();
        }
        try {
            Properties props = new Properties();

            Map<String, String> configs = Map.of("brokerServiceUrl", "https://my-pulsar:8443/",
                    "authPlugin", "org.apache.pulsar.testclient.PerfClientUtilsTest.MyAuth",
                    "authParams", "myparams",
                    "useKeyStoreTls", "true",
                    "tlsTrustStorePath", "./path",
                    "tlsTrustStoreType", "PKCS12",
                    "tlsTrustStorePassword", "changeme",
                    "tlsKeyStorePath", "./path"
            );
            props.putAll(configs);
            FileOutputStream out = new FileOutputStream(tempConfigFile);
            props.store(out, "properties file");
            out.close();
            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(confFile)) {
                prop.load(fis);
            }
            args.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            args.parse(new String[]{});
            args.validate();
            assertTrue(args.useKeyStoreTls);
            assertEquals("PKCS12", args.tlsTrustStoreType);
            assertEquals("./path", args.tlsTrustStorePath);
            assertEquals("changeme", args.tlsTrustStorePassword);
            assertEquals("JKS", args.tlsKeyStoreType);
            assertEquals("./path", args.tlsKeyStorePath);
            assertEquals("", args.tlsKeyStorePassword);

        } catch (IOException e) {
            fail("Error while updating/reading config file");
        } finally {
            tempConfigFile.delete();
        }
    }

    @Test
    public void testStorePasswordsAreMaskedInLoggedConfig() throws Exception {
        final PerformanceBaseArguments args = new PerformanceBaseArguments("") {
            @Override
            public void run() throws Exception {

            }
        };
        args.parse(new String[]{"--use-keystore-tls", "--truststore-pass", "trust-secret",
                "--keystore-pass", "key-secret"});

        // The perf commands log the arguments as JSON at startup.
        final String json = new ObjectMapper().writeValueAsString(args);

        assertTrue(json.contains("\"tlsTrustStorePassword\":\"*****\""), json);
        assertTrue(json.contains("\"tlsKeyStorePassword\":\"*****\""), json);
        assertFalse(json.contains("trust-secret"), json);
        assertFalse(json.contains("key-secret"), json);
    }

    @Test
    public void testStorePasswordsAreHiddenInUsageHelp() throws Exception {
        final PerformanceBaseArguments args = new PerformanceBaseArguments("") {
            @Override
            public void run() throws Exception {

            }
        };
        // client.conf values are the picocli defaults, and pulsar-perf shows default values in its help.
        Properties prop = new Properties();
        prop.setProperty("tlsTrustStoreType", "PKCS12");
        prop.setProperty("tlsTrustStorePassword", "trust-secret");
        prop.setProperty("tlsKeyStorePassword", "key-secret");
        args.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
        args.getCommander().getCommandSpec().usageMessage().showDefaultValues(true);

        final String usage = args.getCommander().getUsageMessage();

        assertTrue(usage.contains("Default: PKCS12"), usage);
        assertFalse(usage.contains("trust-secret"), usage);
        assertFalse(usage.contains("key-secret"), usage);
    }
}
