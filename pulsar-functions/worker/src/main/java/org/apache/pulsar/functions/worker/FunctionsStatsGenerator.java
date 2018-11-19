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

import org.apache.pulsar.functions.instance.FunctionStatsManager;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * A class to generate stats for pulsar functions running on this broker
 */
public class FunctionsStatsGenerator {

    private static final Logger log = LoggerFactory.getLogger(FunctionsStatsGenerator.class);

    public static void generate(WorkerService workerService, String cluster, SimpleTextOutputStream out) {
        // only when worker service is initialized, we generate the stats. otherwise we will get bunch of NPE.
        if (workerService != null && workerService.isInitialized()) {
            // kubernetes runtime factory doesn't support stats collection through worker service
            if (workerService.getFunctionRuntimeManager().getRuntimeFactory() instanceof KubernetesRuntimeFactory) {
                return;
            }

            Map<String, FunctionRuntimeInfo> functionRuntimes
                    = workerService.getFunctionRuntimeManager().getFunctionRuntimeInfos();

            for (Map.Entry<String, FunctionRuntimeInfo> entry : functionRuntimes.entrySet()) {
                String fullyQualifiedInstanceName = entry.getKey();
                FunctionRuntimeInfo functionRuntimeInfo = entry.getValue();
                RuntimeSpawner functionRuntimeSpawner = functionRuntimeInfo.getRuntimeSpawner();

                if (functionRuntimeSpawner != null) {
                    Runtime functionRuntime = functionRuntimeSpawner.getRuntime();
                    if (functionRuntime != null) {
                        try {
                            InstanceCommunication.MetricsData metrics = functionRuntime.getMetrics().get();

                            String tenant = functionRuntimeInfo.getFunctionInstance()
                                    .getFunctionMetaData().getFunctionDetails().getTenant();
                            String namespace = functionRuntimeInfo.getFunctionInstance()
                                    .getFunctionMetaData().getFunctionDetails().getNamespace();
                            String name = functionRuntimeInfo.getFunctionInstance()
                                    .getFunctionMetaData().getFunctionDetails().getName();
                            int instanceId = functionRuntimeInfo.getFunctionInstance().getInstanceId();
                            String qualifiedNamespace = String.format("%s/%s", tenant, namespace);

                            metric(out, cluster, qualifiedNamespace, name, FunctionStatsManager.PULSAR_FUNCTION_METRICS_PREFIX + FunctionStatsManager.PROCESS_LATENCY_MS, instanceId, metrics.getAvgProcessLatency());
                            metric(out, cluster, qualifiedNamespace, name, FunctionStatsManager.PULSAR_FUNCTION_METRICS_PREFIX + FunctionStatsManager.LAST_INVOCATION, instanceId, metrics.getLastInvocation());
                            metric(out, cluster, qualifiedNamespace, name, FunctionStatsManager.PULSAR_FUNCTION_METRICS_PREFIX + FunctionStatsManager.PROCESSED_SUCCESSFULLY_TOTAL, instanceId, metrics.getProcessedSuccessfullyTotal());
                            metric(out, cluster, qualifiedNamespace, name, FunctionStatsManager.PULSAR_FUNCTION_METRICS_PREFIX + FunctionStatsManager.PROCESSED_TOTAL, instanceId, metrics.getProcessedTotal());
                            metric(out, cluster, qualifiedNamespace, name, FunctionStatsManager.PULSAR_FUNCTION_METRICS_PREFIX + FunctionStatsManager.RECEIVED_TOTAL, instanceId, metrics.getReceivedTotal());
                            metric(out, cluster, qualifiedNamespace, name, FunctionStatsManager.PULSAR_FUNCTION_METRICS_PREFIX + FunctionStatsManager.SYSTEM_EXCEPTIONS_TOTAL, instanceId, metrics.getSystemExceptionsTotal());
                            metric(out, cluster, qualifiedNamespace, name, FunctionStatsManager.PULSAR_FUNCTION_METRICS_PREFIX + FunctionStatsManager.USER_EXCEPTIONS_TOTAL, instanceId, metrics.getUserExceptionsTotal());

                            for (Map.Entry<String, Double> userMetricsMapEntry : metrics.getUserMetricsMap().entrySet()) {
                                String userMetricName = userMetricsMapEntry.getKey();
                                Double val = userMetricsMapEntry.getValue();
                                metric(out, cluster, qualifiedNamespace, name, FunctionStatsManager.PULSAR_FUNCTION_METRICS_PREFIX + userMetricName, instanceId, val);
                            }

                        } catch (InterruptedException | ExecutionException e) {
                            log.warn("Failed to collect metrics for function instance {}",
                                    fullyQualifiedInstanceName, e);
                        }
                    }
                }
            }
        }
    }

    private static void metricType(SimpleTextOutputStream stream, String name) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace,
                               String functionName, String metricName, int instanceId, double value) {
        metricType(stream, metricName);
        stream.write(metricName).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace)
                .write("\",name=\"").write(functionName).write("\",instanceId=\"").write(instanceId).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }
}
