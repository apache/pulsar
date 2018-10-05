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
package org.apache.pulsar.functions.worker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.ToString;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class FunctionStatsGeneratorTest {

    @Test
    public void testGenerateFunctionStatsWhenWorkerServiceIsNotInitialized() {
        WorkerService workerService = mock(WorkerService.class);
        when(workerService.isInitialized()).thenReturn(false);
        FunctionsStatsGenerator.generate(
            workerService, "test-cluster", new SimpleTextOutputStream(Unpooled.buffer()));
        verify(workerService, times(1)).isInitialized();
        verify(workerService, times(0)).getFunctionRuntimeManager();
    }

    @Test
    public void testGenerateFunctionStatsOnK8SRuntimeFactory() {
        WorkerService workerService = mock(WorkerService.class);
        when(workerService.isInitialized()).thenReturn(true);
        FunctionRuntimeManager frm = mock(FunctionRuntimeManager.class);
        when(frm.getRuntimeFactory()).thenReturn(mock(KubernetesRuntimeFactory.class));
        when(workerService.getFunctionRuntimeManager()).thenReturn(frm);
        FunctionsStatsGenerator.generate(
            workerService, "test-cluster", new SimpleTextOutputStream(Unpooled.buffer()));
        verify(workerService, times(1)).isInitialized();
        verify(workerService, times(1)).getFunctionRuntimeManager();
        verify(frm, times(0)).getFunctionRuntimeInfos();
    }

    @Test
    public void testFunctionsStatsGenerate() {
        FunctionRuntimeManager functionRuntimeManager = mock(FunctionRuntimeManager.class);
        Map<String, FunctionRuntimeInfo> functionRuntimeInfoMap = new HashMap<>();

        WorkerService workerService = mock(WorkerService.class);
        doReturn(functionRuntimeManager).when(workerService).getFunctionRuntimeManager();
        doReturn(new WorkerConfig()).when(workerService).getWorkerConfig();
        when(workerService.isInitialized()).thenReturn(true);

        CompletableFuture<InstanceCommunication.MetricsData> metricsDataCompletableFuture = new CompletableFuture<>();
        InstanceCommunication.MetricsData metricsData = InstanceCommunication.MetricsData.newBuilder()
                .putMetrics(
                        "__total_processed__",
                        InstanceCommunication.MetricsData.DataDigest.newBuilder()
                                .setCount(100.0).setMax(200.0).setSum(300.0).setMin(0.0).build())
                .putMetrics("__avg_latency_ms__",
                        InstanceCommunication.MetricsData.DataDigest.newBuilder()
                                .setCount(10.0).setMax(20.0).setSum(30.0).setMin(0.0).build())
                .build();

        metricsDataCompletableFuture.complete(metricsData);
        Runtime runtime = mock(Runtime.class);
        doReturn(metricsDataCompletableFuture).when(runtime).getMetrics();

        RuntimeSpawner runtimeSpawner = mock(RuntimeSpawner.class);
        doReturn(runtime).when(runtimeSpawner).getRuntime();

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.Instance instance = Function.Instance.newBuilder()
                .setFunctionMetaData(function1).setInstanceId(0).build();

        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(runtimeSpawner).when(functionRuntimeInfo).getRuntimeSpawner();
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();

        functionRuntimeInfoMap.put(Utils.getFullyQualifiedInstanceId(instance), functionRuntimeInfo);
        doReturn(functionRuntimeInfoMap).when(functionRuntimeManager).getFunctionRuntimeInfos();

        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();
        SimpleTextOutputStream statsOut = new SimpleTextOutputStream(buf);
        FunctionsStatsGenerator.generate(workerService, "default", statsOut);

        String str = buf.toString(Charset.defaultCharset());
        buf.release();
        Map<String, Metric> metrics = parseMetrics(str);

        Assert.assertEquals(metrics.size(), 8);

        Metric m = metrics.get("pulsar_function__total_processed__count");
        assertEquals(m.tags.get("cluster"), "default");
        assertEquals(m.tags.get("instanceId"), "0");
        assertEquals(m.tags.get("name"), "func-1");
        assertEquals(m.tags.get("namespace"), "test-tenant/test-namespace");
        assertEquals(m.value, 100.0);

        m = metrics.get("pulsar_function__total_processed__max");
        assertEquals(m.tags.get("cluster"), "default");
        assertEquals(m.tags.get("instanceId"), "0");
        assertEquals(m.tags.get("name"), "func-1");
        assertEquals(m.tags.get("namespace"), "test-tenant/test-namespace");
        assertEquals(m.value, 200.0);

        m = metrics.get("pulsar_function__total_processed__sum");
        assertEquals(m.tags.get("cluster"), "default");
        assertEquals(m.tags.get("instanceId"), "0");
        assertEquals(m.tags.get("name"), "func-1");
        assertEquals(m.tags.get("namespace"), "test-tenant/test-namespace");
        assertEquals(m.value, 300.0);

        m = metrics.get("pulsar_function__total_processed__min");
        assertEquals(m.tags.get("cluster"), "default");
        assertEquals(m.tags.get("instanceId"), "0");
        assertEquals(m.tags.get("name"), "func-1");
        assertEquals(m.tags.get("namespace"), "test-tenant/test-namespace");
        assertEquals(m.value, 0.0);

        m = metrics.get("pulsar_function__avg_latency_ms__count");
        assertEquals(m.tags.get("cluster"), "default");
        assertEquals(m.tags.get("instanceId"), "0");
        assertEquals(m.tags.get("name"), "func-1");
        assertEquals(m.tags.get("namespace"), "test-tenant/test-namespace");
        assertEquals(m.value, 10.0);

        m = metrics.get("pulsar_function__avg_latency_ms__max");
        assertEquals(m.tags.get("cluster"), "default");
        assertEquals(m.tags.get("instanceId"), "0");
        assertEquals(m.tags.get("name"), "func-1");
        assertEquals(m.tags.get("namespace"), "test-tenant/test-namespace");
        assertEquals(m.value, 20.0);

        m = metrics.get("pulsar_function__avg_latency_ms__sum");
        assertEquals(m.tags.get("cluster"), "default");
        assertEquals(m.tags.get("instanceId"), "0");
        assertEquals(m.tags.get("name"), "func-1");
        assertEquals(m.tags.get("namespace"), "test-tenant/test-namespace");
        assertEquals(m.value, 30.0);

        m = metrics.get("pulsar_function__avg_latency_ms__min");
        assertEquals(m.tags.get("cluster"), "default");
        assertEquals(m.tags.get("instanceId"), "0");
        assertEquals(m.tags.get("name"), "func-1");
        assertEquals(m.tags.get("namespace"), "test-tenant/test-namespace");
        assertEquals(m.value, 0.0);
    }

    /**
     * Hacky parsing of Prometheus text format. Sould be good enough for unit tests
     */
    private static Map<String, Metric> parseMetrics(String metrics) {
        Map<String, Metric> parsed = new HashMap<>();

        // Example of lines are
        // jvm_threads_current{cluster="standalone",} 203.0
        // or
        // pulsar_subscriptions_count{cluster="standalone", namespace="sample/standalone/ns1",
        // topic="persistent://sample/standalone/ns1/test-2"} 0.0 1517945780897
        Pattern pattern = Pattern.compile("^(\\w+)\\{([^\\}]+)\\}\\s(-?[\\d\\w\\.]+)(\\s(\\d+))?$");
        Pattern tagsPattern = Pattern.compile("(\\w+)=\"([^\"]+)\"(,\\s?)?");

        Arrays.asList(metrics.split("\n")).forEach(line -> {
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }
            Matcher matcher = pattern.matcher(line);

            checkArgument(matcher.matches());
            String name = matcher.group(1);

            Metric m = new Metric();
            m.value = Double.valueOf(matcher.group(3));

            String tags = matcher.group(2);
            Matcher tagsMatcher = tagsPattern.matcher(tags);
            while (tagsMatcher.find()) {
                String tag = tagsMatcher.group(1);
                String value = tagsMatcher.group(2);
                m.tags.put(tag, value);
            }

            parsed.put(name, m);
        });

        return parsed;
    }

    @ToString
    static class Metric {
        Map<String, String> tags = new TreeMap<>();
        double value;
    }

}
