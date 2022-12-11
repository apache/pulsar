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
package org.apache.pulsar.io.common;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class IOConfigUtilsTest {

    @Data
    static class TestDefaultConfig {

        @FieldDoc(
                required = true,
                defaultValue = "",
                sensitive = true,
                help = "testRequired"
        )
        protected String testRequired;

        @FieldDoc(
                required = false,
                defaultValue = "defaultStr",
                sensitive = false,
                help = "defaultStr"
        )
        protected String defaultStr;

        @FieldDoc(
                required = false,
                defaultValue = "true",
                sensitive = false,
                help = "defaultBool"
        )
        protected boolean defaultBool;

        @FieldDoc(
                required = false,
                defaultValue = "100",
                sensitive = false,
                help = "defaultInt"
        )
        protected int defaultInt;

        @FieldDoc(
                required = false,
                defaultValue = "100",
                sensitive = false,
                help = "defaultLong"
        )
        protected long defaultLong;

        @FieldDoc(
                required = false,
                defaultValue = "100.12",
                sensitive = false,
                help = "defaultDouble"
        )
        protected double defaultDouble;

        @FieldDoc(
                required = false,
                defaultValue = "100.10",
                sensitive = false,
                help = "defaultFloat"
        )
        protected float defaultFloat;

        @FieldDoc(
                required = false,
                defaultValue = "",
                sensitive = false,
                help = "noDefault"
        )
        protected int noDefault;
    }

    @Data
    static class TestConfig {
        @FieldDoc(
                required = false,
                defaultValue = "",
                sensitive = true,
                help = "password"
        )
        protected String password;

        @FieldDoc(
                required = true,
                defaultValue = "",
                sensitive = false,
                help = ""
        )
        protected String notSensitive;

        /**
         * Non-string secrets are not supported at this moment
         */
        @FieldDoc(
                required = false,
                defaultValue = "",
                sensitive = true,
                help = ""
        )
        protected long sensitiveLong;
    }

    @Data
    static class DerivedConfig extends TestConfig {
        @FieldDoc(
                required = true,
                defaultValue = "",
                sensitive = true,
                help = ""
        )
        protected String moreSensitiveStuff;
    }

    @Data
    static class DerivedDerivedConfig extends DerivedConfig {
        @FieldDoc(
                required = true,
                defaultValue = "",
                sensitive = true,
                help = ""
        )
        protected String derivedDerivedSensitive;
    }

    static class TestSourceContext implements SourceContext {

        static Map<String, String> secretsMap = new HashMap<>();
        static {
            secretsMap.put("password", "my-source-password");
            secretsMap.put("moreSensitiveStuff", "more-sensitive-stuff");
            secretsMap.put("derivedDerivedSensitive", "derived-derived-sensitive");
        }

        @Override
        public int getInstanceId() {
            return 0;
        }

        @Override
        public int getNumInstances() {
            return 0;
        }

        @Override
        public void recordMetric(String metricName, double value) {

        }

        @Override
        public String getOutputTopic() {
            return null;
        }

        @Override
        public SourceConfig getSourceConfig() {
            return null;
        }

        @Override
        public String getTenant() {
            return null;
        }

        @Override
        public String getNamespace() {
            return null;
        }

        @Override
        public String getSourceName() {
            return null;
        }

        @Override
        public Logger getLogger() {
            return null;
        }

        @Override
        public String getSecret(String secretName) {
            return secretsMap.get(secretName);
        }

        @Override
        public void incrCounter(String key, long amount) { }

        @Override
        public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
            return null;
        }

        @Override
        public long getCounter(String key) {
            return 0;
        }

        @Override
        public CompletableFuture<Long> getCounterAsync(String key) {
            return null;
        }

        @Override
        public void putState(String key, ByteBuffer value) {

        }

        @Override
        public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
            return null;
        }

        @Override
        public ByteBuffer getState(String key) {
            return null;
        }

        @Override
        public CompletableFuture<ByteBuffer> getStateAsync(String key) {
            return null;
        }
        
        @Override
        public void deleteState(String key) {
        	
        }
        
        @Override
        public CompletableFuture<Void> deleteStateAsync(String key) {
        	return null;
        }

        @Override
        public <O> TypedMessageBuilder<O> newOutputMessage(String topicName, Schema<O> schema) throws PulsarClientException {
            return null;
        }

        @Override
        public <O> ConsumerBuilder<O> newConsumerBuilder(Schema<O> schema) throws PulsarClientException {
            return null;
        }

        @Override
        public PulsarClient getPulsarClient() {
            return null;
        }
    }

    @Test
    public void testDefaultValue() {

        // test required field.
        Assert.expectThrows(IllegalArgumentException.class,
                () -> IOConfigUtils.loadWithSecrets(new HashMap<>(), TestDefaultConfig.class, new TestSinkContext()));


        // test all default value.
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("testRequired", "test");
        TestDefaultConfig testDefaultConfig =
                IOConfigUtils.loadWithSecrets(configMap, TestDefaultConfig.class, new TestSinkContext());
        Assert.assertEquals(testDefaultConfig.getDefaultStr(), "defaultStr");
        Assert.assertEquals(testDefaultConfig.isDefaultBool(), true);
        Assert.assertEquals(testDefaultConfig.getDefaultInt(), 100);
        Assert.assertEquals(testDefaultConfig.getDefaultLong(), 100);
        Assert.assertEquals(testDefaultConfig.getDefaultDouble(), 100.12,0.00001);
        Assert.assertEquals(testDefaultConfig.getDefaultFloat(), 100.10,0.00001);
        Assert.assertEquals(testDefaultConfig.getNoDefault(), 0);
    }

    @Test
    public void testSourceLoadWithSecrets() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("notSensitive", "foo");
        TestConfig testConfig = IOConfigUtils.loadWithSecrets(configMap, TestConfig.class, new TestSourceContext());

        Assert.assertEquals(testConfig.notSensitive, "foo");
        Assert.assertEquals(testConfig.password, "my-source-password");

        configMap = new HashMap<>();
        configMap.put("notSensitive", "foo");
        configMap.put("password", "another-password");
        configMap.put("sensitiveLong", 5L);

        testConfig = IOConfigUtils.loadWithSecrets(configMap, TestConfig.class, new TestSourceContext());

        Assert.assertEquals(testConfig.notSensitive, "foo");
        Assert.assertEquals(testConfig.password, "my-source-password");
        Assert.assertEquals(testConfig.sensitiveLong, 5L);

        // test derived classes
        configMap = new HashMap<>();
        configMap.put("notSensitive", "foo");
        configMap.put("sensitiveLong", 5L);

        DerivedConfig derivedConfig = IOConfigUtils.loadWithSecrets(configMap, DerivedConfig.class, new TestSourceContext());

        Assert.assertEquals(derivedConfig.notSensitive, "foo");
        Assert.assertEquals(derivedConfig.password, "my-source-password");
        Assert.assertEquals(derivedConfig.sensitiveLong, 5L);
        Assert.assertEquals(derivedConfig.moreSensitiveStuff, "more-sensitive-stuff");

        configMap = new HashMap<>();
        configMap.put("notSensitive", "foo");
        configMap.put("sensitiveLong", 5L);

        DerivedDerivedConfig derivedDerivedConfig  = IOConfigUtils.loadWithSecrets(configMap, DerivedDerivedConfig.class, new TestSourceContext());

        Assert.assertEquals(derivedDerivedConfig.notSensitive, "foo");
        Assert.assertEquals(derivedDerivedConfig.password, "my-source-password");
        Assert.assertEquals(derivedDerivedConfig.sensitiveLong, 5L);
        Assert.assertEquals(derivedDerivedConfig.moreSensitiveStuff, "more-sensitive-stuff");
        Assert.assertEquals(derivedDerivedConfig.derivedDerivedSensitive, "derived-derived-sensitive");
    }

    static class TestSinkContext implements SinkContext {
        static Map<String, String> secretsMap = new HashMap<>();
        static {
            secretsMap.put("password", "my-sink-password");
            secretsMap.put("moreSensitiveStuff", "more-sensitive-stuff");
            secretsMap.put("derivedDerivedSensitive", "derived-derived-sensitive");
        }

        @Override
        public int getInstanceId() {
            return 0;
        }

        @Override
        public int getNumInstances() {
            return 0;
        }

        @Override
        public void recordMetric(String metricName, double value) {

        }

        @Override
        public Collection<String> getInputTopics() {
            return null;
        }

        @Override
        public SinkConfig getSinkConfig() {
            return null;
        }

        @Override
        public String getTenant() {
            return null;
        }

        @Override
        public String getNamespace() {
            return null;
        }

        @Override
        public String getSinkName() {
            return null;
        }

        @Override
        public Logger getLogger() {
            return null;
        }

        @Override
        public String getSecret(String secretName) {
            return secretsMap.get(secretName);
        }

        @Override
        public void incrCounter(String key, long amount) {
        }

        @Override
        public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
            return null;
        }

        @Override
        public long getCounter(String key) {
            return 0;
        }

        @Override
        public CompletableFuture<Long> getCounterAsync(String key) {
            return null;
        }

        @Override
        public void putState(String key, ByteBuffer value) {

        }

        @Override
        public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
            return null;
        }

        @Override
        public ByteBuffer getState(String key) {
            return null;
        }

        @Override
        public CompletableFuture<ByteBuffer> getStateAsync(String key) {
            return null;
        }
        
        @Override
        public void deleteState(String key) {
        	
        }
        
        @Override
        public CompletableFuture<Void> deleteStateAsync(String key) {
        	return null;
        }

        @Override
        public PulsarClient getPulsarClient() {
            return null;
        }
    }

    @Test
    public void testSinkLoadWithSecrets() {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("notSensitive", "foo");
        TestConfig testConfig = IOConfigUtils.loadWithSecrets(configMap, TestConfig.class, new TestSinkContext());

        Assert.assertEquals(testConfig.notSensitive, "foo");
        Assert.assertEquals(testConfig.password, "my-sink-password");

        configMap = new HashMap<>();
        configMap.put("notSensitive", "foo");
        configMap.put("password", "another-password");
        configMap.put("sensitiveLong", 5L);

        testConfig = IOConfigUtils.loadWithSecrets(configMap, TestConfig.class, new TestSinkContext());

        Assert.assertEquals(testConfig.notSensitive, "foo");
        Assert.assertEquals(testConfig.password, "my-sink-password");
        Assert.assertEquals(testConfig.sensitiveLong, 5L);

        // test derived classes
        configMap = new HashMap<>();
        configMap.put("notSensitive", "foo");
        configMap.put("sensitiveLong", 5L);

        DerivedConfig derivedConfig = IOConfigUtils.loadWithSecrets(configMap, DerivedConfig.class, new TestSinkContext());

        Assert.assertEquals(derivedConfig.notSensitive, "foo");
        Assert.assertEquals(derivedConfig.password, "my-sink-password");
        Assert.assertEquals(derivedConfig.sensitiveLong, 5L);
        Assert.assertEquals(derivedConfig.moreSensitiveStuff, "more-sensitive-stuff");

        configMap = new HashMap<>();
        configMap.put("notSensitive", "foo");
        configMap.put("sensitiveLong", 5L);

        DerivedDerivedConfig derivedDerivedConfig  = IOConfigUtils.loadWithSecrets(configMap, DerivedDerivedConfig.class, new TestSinkContext());

        Assert.assertEquals(derivedDerivedConfig.notSensitive, "foo");
        Assert.assertEquals(derivedDerivedConfig.password, "my-sink-password");
        Assert.assertEquals(derivedDerivedConfig.sensitiveLong, 5L);
        Assert.assertEquals(derivedDerivedConfig.moreSensitiveStuff, "more-sensitive-stuff");
        Assert.assertEquals(derivedDerivedConfig.derivedDerivedSensitive, "derived-derived-sensitive");
    }
}
