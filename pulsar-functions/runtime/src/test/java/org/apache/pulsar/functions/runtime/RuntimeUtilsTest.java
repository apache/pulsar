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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.pulsar.common.functions.MemoryLimit;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.stats.FunctionCollectorRegistry;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.proto.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Resources;
import org.apache.pulsar.functions.proto.RetryDetails;
import org.apache.pulsar.functions.proto.SourceSpec;
import org.jose4j.json.internal.json_simple.JSONObject;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import picocli.CommandLine;

@CustomLog
public class RuntimeUtilsTest {

    @Test
    public void testSplitRuntimeArgs() {
        String str1 = "-Xms314572800";
        String[] result = RuntimeUtils.splitRuntimeArgs(str1);
        Assert.assertEquals(result.length, 1);
        Assert.assertEquals(result[0], str1);

        String str2 = "-Xms314572800 -Dbar=foo";
        result = RuntimeUtils.splitRuntimeArgs(str2);
        Assert.assertEquals(result.length, 2);
        Assert.assertEquals(result[0], "-Xms314572800");
        Assert.assertEquals(result[1], "-Dbar=foo");

        String str3 = "-Xms314572800 -Dbar=foo -Dfoo=\"bar foo\"";
        result = RuntimeUtils.splitRuntimeArgs(str3);
        Assert.assertEquals(result.length, 3);
        Assert.assertEquals(result[0], "-Xms314572800");
        Assert.assertEquals(result[1], "-Dbar=foo");
        Assert.assertEquals(result[2], "-Dfoo=\"bar foo\"");
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @Test(dataProvider = "k8sRuntime")
    public void getGoInstanceCmd(boolean k8sRuntime) throws IOException {
        HashMap<String, String> goInstanceConfig;

        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setClusterName("kluster");
        instanceConfig.setInstanceId(3000);
        instanceConfig.setFunctionId("func-7734");
        instanceConfig.setFunctionVersion("1.0.0");
        instanceConfig.setMaxBufferedTuples(5);
        instanceConfig.setPort(1337);
        instanceConfig.setMetricsPort(60000);

        AuthenticationConfig authConfig = AuthenticationConfig.builder()
                .clientAuthenticationPlugin("org.apache.pulsar.client.impl.auth.AuthenticationToken")
                .clientAuthenticationParameters("file:///secret/token.jwt")
                .tlsTrustCertsFilePath("/secret/ca.cert.pem")
                .tlsHostnameVerificationEnable(true)
                .tlsAllowInsecureConnection(false)
                .build();

        JSONObject userConfig = new JSONObject();
        userConfig.put("word-of-the-day", "der Weltschmerz");

        JSONObject secretsMap = new JSONObject();
        secretsMap.put("secret", "cake is a lie");

        SourceSpec sources = new SourceSpec()
                .setCleanupSubscription(true)
                .setSubscriptionName("go-func-sub")
                .setTimeoutMs(500);
        sources.putInputSpecs("go-func-input").setIsRegexPattern(false);

        RetryDetails retryDetails = new RetryDetails()
                .setDeadLetterTopic("go-func-deadletter")
                .setMaxMessageRetries(1);

        Resources resources = new Resources()
                .setCpu(2)
                .setDisk(1024)
                .setRam(32);

        FunctionDetails functionDetails = new FunctionDetails()
                .setAutoAck(true)
                .setTenant("public")
                .setNamespace("default")
                .setName("go-func")
                .setLogTopic("go-func-log")
                .setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE)
                .setRuntime(FunctionDetails.Runtime.GO)
                .setSecretsMap(secretsMap.toJSONString())
                .setParallelism(1)
                .setUserConfig(userConfig.toJSONString());
        functionDetails.setSource().copyFrom(sources);
        functionDetails.setRetryDetails().copyFrom(retryDetails);
        functionDetails.setResources().copyFrom(resources);

        instanceConfig.setFunctionDetails(functionDetails);
        instanceConfig.setExposePulsarAdminClientEnabled(true);

        List<String> commands = RuntimeUtils.getGoInstanceCmd(instanceConfig, authConfig, "config",
                "pulsar://localhost:6650", "bk://localhost:4181",
                "http://localhost:8080", k8sRuntime);
        if (k8sRuntime) {
            goInstanceConfig = new ObjectMapper().readValue(commands.get(2).replaceAll("^\'|\'$", ""), HashMap.class);
        } else {
            goInstanceConfig = new ObjectMapper().readValue(commands.get(2), HashMap.class);
        }
        Assert.assertEquals(commands.toArray().length, 3);
        Assert.assertEquals(commands.get(0), "config");
        Assert.assertEquals(commands.get(1), "-instance-conf");
        Assert.assertEquals(goInstanceConfig.get("maxBufTuples"), 5);
        Assert.assertEquals(goInstanceConfig.get("maxMessageRetries"), 1);
        Assert.assertEquals(goInstanceConfig.get("killAfterIdleMs"), 0);
        Assert.assertEquals(goInstanceConfig.get("parallelism"), 1);
        Assert.assertEquals(goInstanceConfig.get("className"), "");
        Assert.assertEquals(goInstanceConfig.get("sourceSpecsTopic"), "go-func-input");
        Assert.assertEquals(goInstanceConfig.get("secretsMap"), secretsMap.toString());
        Assert.assertEquals(goInstanceConfig.get("sourceSchemaType"), "");
        Assert.assertEquals(goInstanceConfig.get("sinkSpecsTopic"), "");
        Assert.assertEquals(goInstanceConfig.get("clusterName"), "kluster");
        Assert.assertEquals(goInstanceConfig.get("nameSpace"), "default");
        Assert.assertEquals(goInstanceConfig.get("receiverQueueSize"), 0);
        Assert.assertEquals(goInstanceConfig.get("tenant"), "public");
        Assert.assertEquals(goInstanceConfig.get("ram"), 32);
        Assert.assertEquals(goInstanceConfig.get("logTopic"), "go-func-log");
        Assert.assertEquals(goInstanceConfig.get("processingGuarantees"), 0);
        Assert.assertEquals(goInstanceConfig.get("autoAck"), true);
        Assert.assertEquals(goInstanceConfig.get("regexPatternSubscription"), false);
        Assert.assertEquals(goInstanceConfig.get("pulsarServiceURL"), "pulsar://localhost:6650");
        Assert.assertEquals(goInstanceConfig.get("stateStorageServiceUrl"), "bk://localhost:4181");
        Assert.assertEquals(goInstanceConfig.get("pulsarWebServiceUrl"), "http://localhost:8080");
        Assert.assertEquals(goInstanceConfig.get("runtime"), 3);
        Assert.assertEquals(goInstanceConfig.get("cpu"), 2.0);
        Assert.assertEquals(goInstanceConfig.get("funcID"), "func-7734");
        Assert.assertEquals(goInstanceConfig.get("funcVersion"), "1.0.0");
        Assert.assertEquals(goInstanceConfig.get("disk"), 1024);
        Assert.assertEquals(goInstanceConfig.get("instanceID"), 3000);
        Assert.assertEquals(goInstanceConfig.get("cleanupSubscription"), true);
        Assert.assertEquals(goInstanceConfig.get("port"), 1337);
        Assert.assertEquals(goInstanceConfig.get("subscriptionType"), 0);
        Assert.assertEquals(goInstanceConfig.get("timeoutMs"), 500);
        Assert.assertEquals(goInstanceConfig.get("subscriptionName"), "go-func-sub");
        Assert.assertEquals(goInstanceConfig.get("name"), "go-func");
        Assert.assertEquals(goInstanceConfig.get("expectedHealthCheckInterval"), 0);
        Assert.assertEquals(goInstanceConfig.get("deadLetterTopic"), "go-func-deadletter");
        Assert.assertEquals(goInstanceConfig.get("userConfig"), userConfig.toString());
        Assert.assertEquals(goInstanceConfig.get("metricsPort"), 60000);
        Assert.assertEquals(goInstanceConfig.get("clientAuthenticationPlugin"),
                "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        Assert.assertEquals(goInstanceConfig.get("clientAuthenticationParameters"), "file:///secret/token.jwt");
        Assert.assertEquals(goInstanceConfig.get("tlsTrustCertsFilePath"), "/secret/ca.cert.pem");
        Assert.assertEquals(goInstanceConfig.get("tlsHostnameVerificationEnable"), true);
        Assert.assertEquals(goInstanceConfig.get("tlsAllowInsecureConnection"), false);
    }

    @DataProvider(name = "k8sRuntime")
    public static Object[][] k8sRuntimeFlag() {
        return new Object[][] {
                {
                        true
                },
                {
                        false
                }
        };
    }

    @Test(dataProvider = "k8sRuntime")
    public void getAdditionalJavaRuntimeArguments(boolean k8sRuntime) throws Exception {

        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setClusterName("kluster");
        instanceConfig.setInstanceId(3000);
        instanceConfig.setFunctionId("func-7734");
        instanceConfig.setFunctionVersion("1.0.0");
        instanceConfig.setMaxBufferedTuples(5);
        instanceConfig.setPort(1337);
        instanceConfig.setFunctionDetails(new FunctionDetails());
        instanceConfig.setAdditionalJavaRuntimeArguments(Arrays.asList("-XX:+ExitOnOutOfMemoryError"));

        List<String> cmd = RuntimeUtils.getCmd(instanceConfig, "instanceFile",
                "extraDependenciesDir", /* extra dependencies for running instances */
                "logDirectory",
                "originalCodeFileName",
                "originalExtraFileName",
                "pulsarServiceUrl",
                "stateStorageServiceUrl",
                AuthenticationConfig.builder().build(),
                "shardId",
                23,
                1234L,
                "logConfigFile",
                "secretsProviderClassName",
                "secretsProviderConfig",
                false,
                null,
                null,
                "narExtractionDirectory",
                "functionInstanceClassPath",
                false,
                "");

        log.info().attr("cmd", cmd).log("Generated command");

        assertTrue(cmd.contains("-XX:+ExitOnOutOfMemoryError"));

        // verify that the additional runtime arguments are passed before the Java class
        int indexJavaClass = cmd.indexOf("org.apache.pulsar.functions.instance.JavaInstanceMain");
        int indexAdditionalArguments = cmd.indexOf("-XX:+ExitOnOutOfMemoryError");
        assertTrue(indexJavaClass > 0);
        assertTrue(indexAdditionalArguments > 0);
        assertTrue(indexAdditionalArguments < indexJavaClass);
    }

    @DataProvider(name = "pulsarClientMemoryLimit")
    public Object[][] pulsarClientMemoryLimit() {
        return new Object[][] {
                {null},
                {new MemoryLimit().setLimitInBytes(64L * 1024 * 1024)},
                {new MemoryLimit().setPercentOfMaxDirectMemory(25.0)},
                {new MemoryLimit().setLimitInBytes(64L * 1024 * 1024).setPercentOfMaxDirectMemory(25.0)},
        };
    }

    @Test(dataProvider = "pulsarClientMemoryLimit")
    public void testPulsarClientMemoryLimitReachesJavaInstance(MemoryLimit memoryLimit) throws Exception {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setClusterName("kluster");
        instanceConfig.setInstanceId(3000);
        instanceConfig.setFunctionId("func-7734");
        instanceConfig.setFunctionVersion("1.0.0");
        instanceConfig.setMaxBufferedTuples(5);
        instanceConfig.setPort(1337);
        instanceConfig.setFunctionDetails(new FunctionDetails());
        instanceConfig.setPulsarClientMemoryLimit(memoryLimit);

        List<String> cmd = RuntimeUtils.getCmd(instanceConfig, "instanceFile",
                null, "logDirectory", "originalCodeFileName", null, "pulsarServiceUrl",
                null, AuthenticationConfig.builder().build(), "0", 23, 1234L, "logConfigFile",
                null, null, false, null, null, "narExtractionDirectory", "functionInstanceClassPath",
                false, "");

        // the arguments after the main class are the ones JavaInstanceStarter parses in the instance process
        int indexJavaClass = cmd.indexOf("org.apache.pulsar.functions.instance.JavaInstanceMain");
        JavaInstanceStarter javaInstanceStarter = new JavaInstanceStarter();
        new CommandLine(javaInstanceStarter).parseArgs(
                cmd.subList(indexJavaClass + 1, cmd.size()).toArray(new String[0]));

        assertEquals(javaInstanceStarter.pulsarClientMemoryLimitBytes,
                memoryLimit != null ? memoryLimit.getLimitInBytes() : null);
        assertEquals(javaInstanceStarter.pulsarClientMemoryLimitPercent,
                memoryLimit != null ? memoryLimit.getPercentOfMaxDirectMemory() : null);
    }

    @Test
    public void testLog4j2ShutdownHookIsDisabledForJavaInstance() throws Exception {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setClusterName("kluster");
        instanceConfig.setInstanceId(3000);
        instanceConfig.setFunctionId("func-7734");
        instanceConfig.setFunctionVersion("1.0.0");
        instanceConfig.setMaxBufferedTuples(5);
        instanceConfig.setPort(1337);
        instanceConfig.setFunctionDetails(new FunctionDetails());

        List<String> cmd = RuntimeUtils.getCmd(instanceConfig, "instanceFile",
                null, "logDirectory", "originalCodeFileName", null, "pulsarServiceUrl",
                null, AuthenticationConfig.builder().build(), "0", 23, 1234L, "logConfigFile",
                null, null, false, null, null, "narExtractionDirectory", "functionInstanceClassPath",
                false, "");

        // JavaInstanceStarter shuts down Log4j2 after the instance has been closed. If Log4j2 stopped
        // its appenders from its own shutdown hook, logs written by Sink.close() and Function.close() would be lost.
        int indexJavaClass = cmd.indexOf("org.apache.pulsar.functions.instance.JavaInstanceMain");
        int indexShutdownHookOption = cmd.indexOf("-Dlog4j.shutdownHookEnabled=false");
        assertTrue(indexShutdownHookOption > 0);
        assertTrue(indexShutdownHookOption < indexJavaClass);
    }

    @Test
    public void testSanitizeFileName() {
        assertEquals(RuntimeUtils.sanitizeFileName("file(1).jar"), "file_1_.jar");
        assertEquals(RuntimeUtils.sanitizeFileName("äöå.txt"), "aoa.txt");
        assertEquals(RuntimeUtils.sanitizeFileName("ÄÖÅ.txt"), "AOA.txt");
        assertNull(RuntimeUtils.sanitizeFileName(null));
    }

    /**
     * Every LocalRunner start gets a fresh registry. {@code JvmMetrics.builder().register(registry)} would keep
     * each of them in a static set for the JVM's lifetime and skip the JVM families on any registry it had
     * seen, so the collectors are registered one by one. The allocation collector is not among them: it adds a
     * garbage collector listener per registration that nothing removes.
     */
    @Test
    public void testRegisterDefaultCollectorsOnFreshRegistries() throws Exception {
        Field registered = JvmMetrics.class.getDeclaredField("REGISTERED");
        registered.setAccessible(true);
        int registriesHeldByJvmMetrics = ((Set<?>) registered.get(null)).size();

        FunctionCollectorRegistry first = FunctionCollectorRegistry.getDefaultImplementation();
        FunctionCollectorRegistry second = FunctionCollectorRegistry.getDefaultImplementation();
        RuntimeUtils.registerDefaultCollectors(first);
        RuntimeUtils.registerDefaultCollectors(second);

        Set<String> firstFamilies = families(first);
        Set<String> secondFamilies = families(second);
        assertEquals(secondFamilies, firstFamilies);
        // one family per collector registered by registerDefaultCollectors (native memory only reports with
        // -XX:NativeMemoryTracking, so it has no family to check here)
        for (String family : new String[]{"jvm_memory_used_bytes", "jvm_memory_pool_used_bytes", "jvm_threads_current",
                "jvm_gc_collection_seconds", "jvm_classes_currently_loaded", "jvm_buffer_pool_used_bytes",
                "jvm_compilation_time_seconds",
                // registered names: exposed as jvm_runtime_info and process_cpu_seconds_total
                "jvm_runtime", "process_cpu_seconds", "jmx_scrape_duration_seconds"}) {
            assertTrue(firstFamilies.contains(family), family + " missing from " + firstFamilies);
        }
        assertFalse(firstFamilies.contains("jvm_memory_pool_allocated_bytes"),
                "the allocation collector leaves a GC listener behind on every registration");
        // the individual collectors keep no static state; JvmMetrics.REGISTERED must not have grown
        assertEquals(((Set<?>) registered.get(null)).size(), registriesHeldByJvmMetrics);
    }

    private static Set<String> families(PrometheusRegistry registry) {
        MetricSnapshots snapshots = registry.scrape();
        return snapshots.stream().map(MetricSnapshot::getMetadata)
                .map(metadata -> metadata.getPrometheusName()).collect(Collectors.toSet());
    }
}
