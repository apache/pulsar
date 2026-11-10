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
package org.apache.pulsar.functions.instance.stats;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ComponentStatsManagerTest {

    private static final String LABELS = "{cluster=\"standalone\",fqfn=\"public/default/fn\",instance_id=\"0\","
            + "name=\"fn\",namespace=\"public/default\",tenant=\"public\"}";

    private ScheduledExecutorService executor;

    @BeforeMethod
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        executor.shutdownNow();
    }

    private static String[] labels(String instanceId) {
        return new String[]{"public", "public/default", "fn", instanceId, "standalone", "public/default/fn"};
    }

    @Test
    public void testFunctionExposition() throws Exception {
        FunctionCollectorRegistry registry = FunctionCollectorRegistry.getDefaultImplementation();
        ComponentStatsManager stats = ComponentStatsManager.getStatsManager(registry, labels("0"), executor,
                FunctionDetails.ComponentType.FUNCTION);

        stats.incrTotalReceived();
        stats.incrTotalProcessedSuccessfully();
        stats.incrUserExceptions(new RuntimeException("boom"));
        stats.setLastInvocation(1700000000000L);
        // start timestamps 5 ms and 15 ms in the past: the recorded latencies are at least that, in milliseconds
        stats.processTimeEnd(System.nanoTime() - 5_000_000L);
        stats.processTimeEnd(System.nanoTime() - 15_000_000L);

        String text = stats.getStatsAsString();
        assertThat(text)
                .contains("# TYPE pulsar_function_received_total counter\n")
                .contains("pulsar_function_received_total" + LABELS + " 1.0\n")
                .contains("pulsar_function_received_1min_total" + LABELS + " 1.0\n")
                .contains("pulsar_function_processed_successfully_total" + LABELS + " 1.0\n")
                .contains("pulsar_function_user_exceptions_total" + LABELS + " 1.0\n")
                .contains("pulsar_function_system_exceptions_total" + LABELS + " 0.0\n")
                .contains("# TYPE pulsar_function_last_invocation gauge\n")
                .contains("pulsar_function_last_invocation" + LABELS + " 1.7E12\n")
                .contains("# TYPE pulsar_function_process_latency_ms summary\n")
                .contains("pulsar_function_process_latency_ms_count" + LABELS + " 2\n")
                .contains("pulsar_function_process_latency_ms{cluster=\"standalone\",fqfn=\"public/default/fn\","
                        + "instance_id=\"0\",name=\"fn\",namespace=\"public/default\",tenant=\"public\","
                        + "quantile=\"0.999\"} ")
                .contains("# TYPE pulsar_function_user_exception gauge\n")
                .contains("pulsar_function_user_exception{cluster=\"standalone\",error=\"boom\","
                        + "fqfn=\"public/default/fn\",instance_id=\"0\",name=\"fn\",namespace=\"public/default\","
                        + "tenant=\"public\"} 1.0\n")
                .doesNotContain("_created")
                // never set, so the family has no data point and is not printed
                .doesNotContain("pulsar_function_source_exception");

        assertThat(stats.getTotalRecordsReceived()).isEqualTo(1.0);
        assertThat(stats.getTotalUserExceptions1min()).isEqualTo(1.0);
        // nanoseconds are converted to milliseconds: the average is at least 10 ms and nowhere near 10 s
        assertThat(stats.getAvgProcessLatency()).isBetween(10.0, 5_000.0);
        assertThat(stats.getAvgProcessLatency1min()).isBetween(10.0, 5_000.0);
        assertThat(stats.getLatestUserExceptions()).hasSize(1);

        stats.reset();
        assertThat(stats.getTotalUserExceptions1min()).isEqualTo(0.0);
        assertThat(stats.getTotalUserExceptions()).isEqualTo(1.0);
        assertThat(stats.getAvgProcessLatency1min()).isEqualTo(0.0);
        assertThat(stats.getStatsAsString()).contains("pulsar_function_user_exceptions_1min_total" + LABELS + " 0.0\n")
                .contains("pulsar_function_process_latency_ms_1min{cluster=\"standalone\",fqfn=\"public/default/fn\","
                        + "instance_id=\"0\",name=\"fn\",namespace=\"public/default\",tenant=\"public\","
                        + "quantile=\"0.5\"} NaN\n");
        stats.close();
    }

    @Test
    public void testSinkAndSourceExposition() throws Exception {
        FunctionCollectorRegistry registry = FunctionCollectorRegistry.getDefaultImplementation();
        ComponentStatsManager sink = ComponentStatsManager.getStatsManager(registry, labels("0"), executor,
                FunctionDetails.ComponentType.SINK);
        ComponentStatsManager source = ComponentStatsManager.getStatsManager(registry, labels("0"), executor,
                FunctionDetails.ComponentType.SOURCE);

        sink.incrTotalReceived();
        sink.incrTotalProcessedSuccessfully();
        sink.incrSinkExceptions(new RuntimeException("sink failed"));
        source.incrTotalProcessedSuccessfully();
        source.incrSourceExceptions(new RuntimeException("source failed"));

        String text = sink.getStatsAsString();
        assertThat(text)
                .contains("pulsar_sink_received_total" + LABELS + " 1.0\n")
                .contains("pulsar_sink_written_total" + LABELS + " 1.0\n")
                .contains("pulsar_sink_written_1min_total" + LABELS + " 1.0\n")
                .contains("pulsar_sink_sink_exceptions_total" + LABELS + " 1.0\n")
                .contains("pulsar_sink_sink_exceptions_1min_total" + LABELS + " 1.0\n")
                .contains("# TYPE pulsar_sink_sink_exception gauge\n")
                .contains("pulsar_sink_sink_exception{cluster=\"standalone\",error=\"sink failed\",")
                .contains("pulsar_source_written_total" + LABELS + " 1.0\n")
                .contains("pulsar_source_source_exceptions_total" + LABELS + " 1.0\n")
                .contains("pulsar_source_source_exception{cluster=\"standalone\",error=\"source failed\",");
        assertThat(sink.getLatestSinkExceptions()).hasSize(1);
        assertThat(source.getLatestSourceExceptions()).hasSize(1);
        sink.close();
        source.close();
    }

    @Test
    public void testInstancesShareFamiliesOnOneRegistry() throws Exception {
        FunctionCollectorRegistry registry = FunctionCollectorRegistry.getDefaultImplementation();
        ComponentStatsManager first = ComponentStatsManager.getStatsManager(registry, labels("0"), executor,
                FunctionDetails.ComponentType.FUNCTION);
        ComponentStatsManager second = ComponentStatsManager.getStatsManager(registry, labels("1"), executor,
                FunctionDetails.ComponentType.FUNCTION);
        first.incrTotalReceived();
        second.incrTotalReceived();
        second.incrTotalReceived();

        String text = first.getStatsAsString();
        assertThat(text)
                .containsOnlyOnce("# TYPE pulsar_function_received_total counter\n")
                .contains("pulsar_function_received_total" + LABELS + " 1.0\n")
                .contains("pulsar_function_received_total" + LABELS.replace("instance_id=\"0\"", "instance_id=\"1\"")
                        + " 2.0\n");
        assertThat(first.getTotalRecordsReceived()).isEqualTo(1.0);
        assertThat(second.getTotalRecordsReceived()).isEqualTo(2.0);
        first.close();
        second.close();
    }
}
