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
package org.apache.pulsar.functions.runtime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


import com.beust.jcommander.ParameterException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.assertj.core.util.Sets;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.validation.ValidationException;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

@Slf4j
public class JavaInstanceStarterTest {
    
    private static String STRING_INVALID_VALUE = "invalidString";
    
    private static String INTEGER_INVALID_VALUE = "-1";


    @Test
    public void fileConfigShouldConflictWithRequiredCommandlineConfig() throws IOException {
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
            .withJCommanderConfigs(requiredJCommanderConfigs())
            .useFile()
            .build();

        Assert.assertThrows(ValidationException.class, () -> javaInstanceStarter.setConfigs(args));
    }

    @Test
    public void fileConfigShouldConfigWithAnyCommandLineConfig() throws IOException {
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
            .withJCommanderConfigs("--use_tls", Boolean.TRUE.toString())
            .useFile()
            .build();

        Assert.assertThrows(ValidationException.class, () -> javaInstanceStarter.setConfigs(args));
    }

    @Test
    public void useBooleanStringShouldSetArityOneBooleanField() throws IOException {
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
                .withJCommanderConfigs(requiredJCommanderConfigs())
                .withJCommanderConfigs("--use_tls", Boolean.TRUE.toString())
                .build();
        javaInstanceStarter.setConfigs(args);
        assertRequiredFieldsAreSet(javaInstanceStarter);
        Assert.assertEquals(javaInstanceStarter.useTls, Boolean.TRUE.toString());
    }

    @Test
    public void useFlagShouldSetArityZeroBooleanField() throws IOException {
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
                .withJCommanderConfigs(requiredJCommanderConfigs())
                .arityZeroBooleanInJCommander()
                .build();
        javaInstanceStarter.setConfigs(args);
        assertRequiredFieldsAreSet(javaInstanceStarter);
        Assert.assertEquals(javaInstanceStarter.exposePulsarAdminClientEnabled, Boolean.TRUE);
    }

    @Test
    public void missingRequiredCommandLineArgsShouldFail() throws IOException {
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
            .withJCommanderConfigs(optionalJCommanderConfigs())
            .build();

        Assert.assertThrows(ParameterException.class, () -> javaInstanceStarter.setConfigs(args));
    }

    @Test
    public void shouldSetDefaultCommandLineArgs() throws IOException {
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
            .withJCommanderConfigs(requiredJCommanderConfigs())
            .build();
        javaInstanceStarter.setConfigs(args);
        assertOptionalFieldHasDefaultValue(javaInstanceStarter);
    }

    @Test
    public void allOptionsCanBeSet() throws IOException {
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
            .withJCommanderConfigs(requiredJCommanderConfigs())
            .withJCommanderConfigs(optionalJCommanderConfigs())
            .arityZeroBooleanInJCommander()
            .build();
        javaInstanceStarter.setConfigs(args);
        assertRequiredFieldsAreSet(javaInstanceStarter);
        assertOptionalFieldsAreSet(javaInstanceStarter);
    }

    @Test
    public void fileConfigShouldCheckRequiredFields() throws IOException {
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
            .useFile()
            .build();
        Assert.assertThrows(ParameterException.class, () -> javaInstanceStarter.setConfigs(args));
    }

    @Test
    public void fileConfigShouldProvideSameDefaultValue() throws IOException {
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
            .useFile()
            .withFileConfigs(requiredFileConfigs())
            .build();
        javaInstanceStarter.setConfigs(args);
        assertRequiredFieldsAreSet(javaInstanceStarter);
        assertOptionalFieldHasDefaultValue(javaInstanceStarter);
    }

    @Test
    public void invalidFileLocationShouldThrowException() throws IOException{
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
            .withJCommanderConfigs("--config_file", "invalid_file")
            .build();
        Assert.assertThrows(FileNotFoundException.class, () -> javaInstanceStarter.setConfigs(args));
    }

    @Test
    public void invalidFileConfigShouldBeIgnored() {

    }

    @Test
    public void allOptionsInFileConfigCanBeSet() throws IOException{
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        String[] args = new CommandLineArgsBuilder()
            .withFileConfigs(requiredFileConfigs())
            .withFileConfigs(optionalFileConfigs())
            .build();
        javaInstanceStarter.setConfigs(args);
        assertRequiredFieldsAreSet(javaInstanceStarter);
        assertOptionalFieldsAreSet(javaInstanceStarter);
    }

    private void assertRequiredFieldsAreSet(JavaInstanceStarter javaInstanceStarter) {
        // Required String
        Assert.assertEquals(javaInstanceStarter.functionDetailsJsonString, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.functionId, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.functionVersion, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.pulsarServiceUrl, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.clusterName, STRING_INVALID_VALUE);
        // Required Integer
        Assert.assertEquals(javaInstanceStarter.instanceId, Integer.valueOf(INTEGER_INVALID_VALUE));
        Assert.assertEquals(javaInstanceStarter.port, Integer.valueOf(INTEGER_INVALID_VALUE));
        Assert.assertEquals(javaInstanceStarter.metricsPort, Integer.valueOf(INTEGER_INVALID_VALUE));
        Assert.assertEquals(javaInstanceStarter.maxBufferedTuples, Integer.valueOf(INTEGER_INVALID_VALUE));
        Assert.assertEquals(javaInstanceStarter.expectedHealthCheckInterval, Integer.valueOf(INTEGER_INVALID_VALUE));

    }

    private void assertOptionalFieldsAreSet(JavaInstanceStarter javaInstanceStarter) {
        // Optional String
        Assert.assertEquals(javaInstanceStarter.jarFile, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.transformFunctionJarFile, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.transformFunctionId, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.clientAuthenticationPlugin, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.clientAuthenticationParameters, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.tlsTrustCertFilePath, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.stateStorageImplClass, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.stateStorageServiceUrl, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.secretsProviderClassName, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.secretsProviderConfig, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.narExtractionDirectory, STRING_INVALID_VALUE);
        Assert.assertEquals(javaInstanceStarter.webServiceUrl, STRING_INVALID_VALUE);
        // Optional Integer
        Assert.assertEquals(javaInstanceStarter.maxPendingAsyncRequests, Integer.valueOf(INTEGER_INVALID_VALUE));

        // Optional Boolean
        Assert.assertEquals(javaInstanceStarter.useTls, Boolean.TRUE.toString());
        Assert.assertEquals(javaInstanceStarter.tlsAllowInsecureConnection, Boolean.FALSE.toString());
        Assert.assertEquals(javaInstanceStarter.tlsHostNameVerificationEnabled, Boolean.TRUE.toString());
        Assert.assertEquals(javaInstanceStarter.exposePulsarAdminClientEnabled, Boolean.TRUE);
    }

    private void assertOptionalFieldHasDefaultValue(JavaInstanceStarter javaInstanceStarter) {
        // Optional String
        Assert.assertNull(javaInstanceStarter.jarFile);
        Assert.assertNull(javaInstanceStarter.transformFunctionJarFile);
        Assert.assertNull(javaInstanceStarter.transformFunctionId);
        Assert.assertNull(javaInstanceStarter.clientAuthenticationPlugin);
        Assert.assertNull(javaInstanceStarter.clientAuthenticationParameters);
        Assert.assertNull(javaInstanceStarter.tlsTrustCertFilePath);
        Assert.assertNull(javaInstanceStarter.stateStorageImplClass);
        Assert.assertNull(javaInstanceStarter.stateStorageServiceUrl);
        Assert.assertNull(javaInstanceStarter.secretsProviderClassName);
        Assert.assertNull(javaInstanceStarter.secretsProviderConfig);
        Assert.assertEquals(javaInstanceStarter.narExtractionDirectory, NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR);
        Assert.assertNull(javaInstanceStarter.webServiceUrl);
        // Optional Integer
        Assert.assertEquals(javaInstanceStarter.maxPendingAsyncRequests, Integer.valueOf(1000));
        // Optional Boolean
        Assert.assertEquals(javaInstanceStarter.useTls, Boolean.FALSE.toString());
        Assert.assertEquals(javaInstanceStarter.tlsAllowInsecureConnection, Boolean.TRUE.toString());
        Assert.assertEquals(javaInstanceStarter.tlsHostNameVerificationEnabled, Boolean.FALSE.toString());
        Assert.assertEquals(javaInstanceStarter.exposePulsarAdminClientEnabled, Boolean.FALSE);
    }

    private Map<String, String> requiredJCommanderConfigs() {
        return new HashMap<>(){{
            // String
            put("--function_details", STRING_INVALID_VALUE);
            put("--function_id", STRING_INVALID_VALUE);
            put("--function_version", STRING_INVALID_VALUE);
            put("--pulsar_serviceurl", STRING_INVALID_VALUE);
            put("--cluster_name", STRING_INVALID_VALUE);
            // Integer
            put("--instance_id", INTEGER_INVALID_VALUE);
            put("--port", INTEGER_INVALID_VALUE);
            put("--metrics_port", INTEGER_INVALID_VALUE);
            put("--max_buffered_tuples", INTEGER_INVALID_VALUE);
            put("--expected_healthcheck_interval", INTEGER_INVALID_VALUE);
        }};
    }

    private Map<String, String> requiredFileConfigs() {
        return new HashMap<>(){{
            // String
            put("functionDetailsJsonString", STRING_INVALID_VALUE);
            put("functionId", STRING_INVALID_VALUE);
            put("functionVersion", STRING_INVALID_VALUE);
            put("pulsarServiceUrl", STRING_INVALID_VALUE);
            put("clusterName", STRING_INVALID_VALUE);
            // Integer
            put("instanceId", INTEGER_INVALID_VALUE);
            put("port", INTEGER_INVALID_VALUE);
            put("metricsPort", INTEGER_INVALID_VALUE);
            put("maxBufferedTuples", INTEGER_INVALID_VALUE);
            put("expectedHealthCheckInterval", INTEGER_INVALID_VALUE);
        }};
    }

    private Map<String, String> optionalJCommanderConfigs() {
        return new HashMap<>(){{
            // String
            put("--jar", STRING_INVALID_VALUE);
            put("--transform_function_jar", STRING_INVALID_VALUE);
            put("--transform_function_id", STRING_INVALID_VALUE);
            put("--client_auth_plugin", STRING_INVALID_VALUE);
            put("--client_auth_params", STRING_INVALID_VALUE);
            put("--tls_trust_cert_path", STRING_INVALID_VALUE);
            put("--state_storage_impl_class", STRING_INVALID_VALUE);
            put("--state_storage_serviceurl", STRING_INVALID_VALUE);
            put("--secrets_provider", STRING_INVALID_VALUE);
            put("--secrets_provider_config", STRING_INVALID_VALUE);
            put("--nar_extraction_directory", STRING_INVALID_VALUE);
            put("--web_serviceurl", STRING_INVALID_VALUE);
            // Integer
            put("--pending_async_requests", INTEGER_INVALID_VALUE);
            // Boolean
            put("--use_tls", Boolean.TRUE.toString());
            put("--tls_allow_insecure", Boolean.FALSE.toString());
            put("--hostname_verification_enabled", Boolean.TRUE.toString());
        }};
    }

    private Map<String, String> optionalFileConfigs() {
        return new HashMap<>(){{
            // String
            put("jarFile", STRING_INVALID_VALUE);
            put("transformFunctionJarFile", STRING_INVALID_VALUE);
            put("transformFunctionId", STRING_INVALID_VALUE);
            put("clientAuthenticationPlugin", STRING_INVALID_VALUE);
            put("clientAuthenticationParameters", STRING_INVALID_VALUE);
            put("tlsTrustCertFilePath", STRING_INVALID_VALUE);
            put("stateStorageImplClass", STRING_INVALID_VALUE);
            put("stateStorageServiceUrl", STRING_INVALID_VALUE);
            put("secretsProviderClassName", STRING_INVALID_VALUE);
            put("secretsProviderConfig", STRING_INVALID_VALUE);
            put("narExtractionDirectory", STRING_INVALID_VALUE);
            put("webServiceUrl", STRING_INVALID_VALUE);
            // Integer
            put("maxPendingAsyncRequests", INTEGER_INVALID_VALUE);
            // Boolean, value different from the default value
            put("useTls", Boolean.TRUE.toString());
            put("tlsAllowInsecureConnection", Boolean.FALSE.toString());
            put("tlsHostNameVerificationEnabled", Boolean.TRUE.toString());
            put("exposePulsarAdminClientEnabled", Boolean.TRUE.toString());
        }};
    }

    @Test
    public void testRequiredConfigFieldNames() {
        JavaInstanceStarter jc = new JavaInstanceStarter();
        assertSetContainsSame(jc.requiredConfigFieldNames(), requiredFileConfigs().keySet());
    }

    @Test
    public void testOptionalConfigFieldNames() {
        JavaInstanceStarter jc = new JavaInstanceStarter();
        Set<String> expected = Sets.newHashSet();
        expected.add("configFile");
        expected.addAll(optionalFileConfigs().keySet());
        assertSetContainsSame(jc.optionalConfigFieldNames(), expected);
    }

    @Test
    public void testRequiredConfigLongestArgNames() {
        JavaInstanceStarter jc = new JavaInstanceStarter();
        assertSetContainsSame(jc.requiredConfigLongestArgNames(), requiredJCommanderConfigs().keySet());
    }

    @Test
    public void testOptionalConfigLongestArgName() {
        JavaInstanceStarter jc = new JavaInstanceStarter();
        Set<String> expected = Sets.newHashSet();
        expected.add("--config_file");
        expected.add("--expose_pulsaradmin");
        expected.addAll(optionalJCommanderConfigs().keySet());
        assertSetContainsSame(jc.optionalConfigLongestArgNames(), expected);
    }

    private void assertSetContainsSame(Set<String> actual, Set<String> expected) {
        Assert.assertTrue(actual.containsAll(expected));
        Assert.assertTrue(expected.containsAll(actual));
    }

    private static String writeConfigsToFile(String name, Map<String, String> configs) throws IOException {
        File runnerConfFile = File.createTempFile(name, ".conf");
        runnerConfFile.deleteOnExit();
        try (FileWriter fileWriter = new FileWriter(runnerConfFile, Charset.defaultCharset())) {
            for (Map.Entry<String, String> entry : configs.entrySet()) {
                fileWriter.write(entry.getKey() + "=" + entry.getValue() + System.getProperty("line.separator"));
            }
        }
        return Paths.get(runnerConfFile.toString()).toAbsolutePath().toString();
    }

    private Map<String, String> setRandomValueForConfigs(Map<String, String> configs) {
        configs.replaceAll((k, v) -> {
            if (Objects.equals(v, INTEGER_INVALID_VALUE)) {
                return String.valueOf(1);
            } else if (Objects.equals(v, STRING_INVALID_VALUE)) {
                return "valueFromFile";
            } else {
                return "true";
            }
        });
        return configs;
    }

    public static class CommandLineArgsBuilder {
        private Map<String, String> jCommanderConfigs;

        private Map<String, String> fileConfigs;

        private Boolean useFile = false;

        private Boolean arityZeroBooleanInJCommander = false;

        private CommandLineArgsBuilder(){
            this.jCommanderConfigs = new HashMap<>();
            this.fileConfigs = new HashMap<>();
        };

        public CommandLineArgsBuilder newBuilder() {
            return new CommandLineArgsBuilder();
        }


        public CommandLineArgsBuilder withJCommanderConfigs(Map<String, String> jCommanderConfigs) {
            this.jCommanderConfigs.putAll(jCommanderConfigs);
            return this;
        }

        public CommandLineArgsBuilder withJCommanderConfigs(String key, String value) {
            this.jCommanderConfigs.put(key, value);
            return this;
        }

        public CommandLineArgsBuilder exceptJCommanderConfigWithKey(String key) {
            this.jCommanderConfigs.remove(key);
            return this;
        }

        public CommandLineArgsBuilder withFileConfigs(Map<String, String> jCommanderConfigs) {
            this.useFile = true;
            this.fileConfigs.putAll(jCommanderConfigs);
            return this;
        }

        public CommandLineArgsBuilder withFileConfigs(String key, String value) {
            this.useFile = true;
            this.fileConfigs.put(key, value);
            return this;
        }

        public CommandLineArgsBuilder exceptFileConfigWithKey(String key) {
            this.fileConfigs.remove(key);
            return this;
        }

        public CommandLineArgsBuilder useFile() {
            this.useFile = true;
            return this;
        }

        public CommandLineArgsBuilder arityZeroBooleanInJCommander() {
            this.arityZeroBooleanInJCommander = true;
            return this;
        }

        public String[] build() throws IOException {
            List<String> result = new ArrayList<>();
            for (Map.Entry<String, String> entry : jCommanderConfigs.entrySet()) {
                result.add(entry.getKey());
                result.add(entry.getValue());
            }

            if (arityZeroBooleanInJCommander) {
                result.add("--expose_pulsaradmin");
            }

            if (useFile) {
                String fileName = randomAlphabetic(7);
                String completeFileName = writeConfigsToFile(fileName, fileConfigs);
                result.add("--config_file");
                result.add(completeFileName);
            }
            return result.toArray(new String[0]);
        }
    }
}
