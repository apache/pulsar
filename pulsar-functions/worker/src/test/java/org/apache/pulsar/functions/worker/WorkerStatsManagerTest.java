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
package org.apache.pulsar.functions.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.testng.annotations.Test;

/**
 * The worker's own families are rendered from a private registry into the text that the worker and
 * broker /metrics append verbatim, so the exposition itself is what callers depend on.
 */
public class WorkerStatsManagerTest {

    private static final String PREFIX = "pulsar_function_worker_";
    private static final String[] DURATION_SUMMARIES = {
            "schedule_execution_time_total_ms",
            "schedule_strategy_execution_time_ms",
            "rebalance_execution_time_total_ms",
            "rebalance_strategy_execution_time_ms",
            "stop_instance_process_time_ms",
            "drain_execution_time_total_ms",
    };

    @Test
    public void testStatsAsStringKeepsNamesLabelsAndQuantiles() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig().setPulsarFunctionsCluster("test-cluster");
        WorkerStatsManager statsManager = new WorkerStatsManager(workerConfig, false);
        FunctionRuntimeManager runtimeManager = mock(FunctionRuntimeManager.class);
        when(runtimeManager.getMyInstances()).thenReturn(3);
        statsManager.setFunctionRuntimeManager(runtimeManager);
        FunctionMetaDataManager metaDataManager = mock(FunctionMetaDataManager.class);
        FunctionMetaData function1 = new FunctionMetaData();
        function1.setFunctionDetails().setParallelism(2);
        FunctionMetaData function2 = new FunctionMetaData();
        function2.setFunctionDetails().setParallelism(3);
        when(metaDataManager.getAllFunctionMetaData()).thenReturn(List.of(function1, function2));
        statsManager.setFunctionMetaDataManager(metaDataManager);
        statsManager.setIsLeader(() -> true);

        statsManager.startupTimeStart();
        statsManager.startupTimeEnd();
        statsManager.scheduleTotalExecTimeStart();
        statsManager.scheduleTotalExecTimeEnd();
        statsManager.scheduleStrategyExecTimeStartStart();
        statsManager.scheduleStrategyExecTimeStartEnd();
        statsManager.rebalanceTotalExecTimeStart();
        statsManager.rebalanceTotalExecTimeEnd();
        statsManager.rebalanceStrategyExecTimeStart();
        statsManager.rebalanceStrategyExecTimeEnd();
        statsManager.stopInstanceProcessTimeStart();
        statsManager.stopInstanceProcessTimeEnd();
        statsManager.drainTotalExecTimeStart();
        statsManager.drainTotalExecTimeEnd();

        String text;
        try {
            text = statsManager.getStatsAsString();
        } finally {
            statsManager.close();
        }

        for (String summary : DURATION_SUMMARIES) {
            String name = PREFIX + summary;
            assertTrue(text.contains("# TYPE " + name + " summary\n"), text);
            for (String quantile : new String[] {"0.5", "0.9", "1.0"}) {
                assertTrue(sample(text, name, "cluster=\"test-cluster\",quantile=\"" + quantile + "\"") > 0,
                        name + " quantile " + quantile + "\n" + text);
            }
            assertEquals(sample(text, name + "_count", "cluster=\"test-cluster\""), 1.0, text);
            assertTrue(sample(text, name + "_sum", "cluster=\"test-cluster\"") > 0, text);
        }

        // never observed: quantiles are NaN, count stays 0
        String idle = PREFIX + "start_instance_process_time_ms";
        assertTrue(text.contains("# TYPE " + idle + " summary\n"), text);
        assertTrue(Double.isNaN(sample(text, idle, "cluster=\"test-cluster\",quantile=\"0.5\"")), text);
        assertEquals(sample(text, idle + "_count", "cluster=\"test-cluster\""), 0.0, text);

        // the startup summary has no quantiles
        String startup = PREFIX + "start_up_time_ms";
        assertTrue(text.contains("# TYPE " + startup + " summary\n"), text);
        assertFalse(text.contains(startup + "{cluster=\"test-cluster\",quantile="), text);
        assertEquals(sample(text, startup + "_count", "cluster=\"test-cluster\""), 1.0, text);

        assertTrue(text.contains("# TYPE " + PREFIX + "instance_count gauge\n"), text);
        assertEquals(sample(text, PREFIX + "instance_count", "cluster=\"test-cluster\""), 3.0, text);

        // leader metrics keep their hand-written form
        assertTrue(text.contains("# TYPE " + PREFIX + "total_function_count gauge\n"
                + PREFIX + "total_function_count{cluster=\"test-cluster\",} 2\n"), text);
        assertTrue(text.contains(PREFIX + "total_expected_instance_count{cluster=\"test-cluster\",} 5\n"), text);
        assertTrue(text.contains(PREFIX + "is_leader{cluster=\"test-cluster\",} 1\n"), text);

        assertFalse(text.contains("_created"), text);
    }

    @Test
    public void testOwnFamiliesStayOffTheDefaultRegistry() {
        WorkerConfig workerConfig = new WorkerConfig().setPulsarFunctionsCluster("test-cluster");
        try (WorkerStatsManager ignored = new WorkerStatsManager(workerConfig, false);
             WorkerStatsManager alsoIgnored = new WorkerStatsManager(workerConfig, false)) {
            for (MetricSnapshot snapshot : PrometheusRegistry.defaultRegistry.scrape()) {
                assertFalse(snapshot.getMetadata().getPrometheusName().startsWith(PREFIX),
                        snapshot.getMetadata().getPrometheusName());
            }
        }
    }

    private static double sample(String text, String name, String labels) {
        Matcher matcher = Pattern.compile("^" + Pattern.quote(name + "{" + labels + "}") + " (\\S+)$",
                Pattern.MULTILINE).matcher(text);
        assertTrue(matcher.find(), "missing sample " + name + "{" + labels + "}\n" + text);
        return Double.parseDouble(matcher.group(1));
    }
}
