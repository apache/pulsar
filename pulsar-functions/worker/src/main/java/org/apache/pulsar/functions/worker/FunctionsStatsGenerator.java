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

import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
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
                            InstanceCommunication.MetricsData metrics = workerService.getWorkerConfig()
                                    .getMetricsSamplingPeriodSec() > 0 ? functionRuntime.getMetrics().get()
                                            : functionRuntime.getAndResetMetrics().get();
                            for (Map.Entry<String, InstanceCommunication.MetricsData.DataDigest> metricsEntry
                                    : metrics.getMetricsMap().entrySet()) {
                                String metricName = metricsEntry.getKey();
                                InstanceCommunication.MetricsData.DataDigest dataDigest = metricsEntry.getValue();

                                String tenant = functionRuntimeInfo.getFunctionInstance()
                                        .getFunctionMetaData().getFunctionDetails().getTenant();
                                String namespace = functionRuntimeInfo.getFunctionInstance()
                                        .getFunctionMetaData().getFunctionDetails().getNamespace();
                                String name = functionRuntimeInfo.getFunctionInstance()
                                        .getFunctionMetaData().getFunctionDetails().getName();
                                int instanceId = functionRuntimeInfo.getFunctionInstance().getInstanceId();
                                String qualifiedNamespace = String.format("%s/%s", tenant, namespace);

                                metric(out, cluster, qualifiedNamespace, name, String.format("pulsar_function%scount", metricName),
                                        instanceId, dataDigest.getCount());
                                metric(out, cluster, qualifiedNamespace, name, String.format("pulsar_function%smax", metricName),
                                        instanceId, dataDigest.getMax());
                                metric(out, cluster, qualifiedNamespace,name, String.format("pulsar_function%smin", metricName),
                                        instanceId, dataDigest.getMin());
                                metric(out, cluster, qualifiedNamespace, name, String.format("pulsar_function%ssum", metricName),
                                        instanceId, dataDigest.getSum());

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
