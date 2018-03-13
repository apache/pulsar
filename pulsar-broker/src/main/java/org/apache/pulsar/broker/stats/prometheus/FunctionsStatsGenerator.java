package org.apache.pulsar.broker.stats.prometheus;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.worker.FunctionRuntimeInfo;
import org.apache.pulsar.utils.SimpleTextOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * A class to generate stats for pulsar functions running on this broker
 */
public class FunctionsStatsGenerator {

    private static final Logger log = LoggerFactory.getLogger(FunctionsStatsGenerator.class);

    public static void generate(PulsarService pulsar, SimpleTextOutputStream out) {
        if (pulsar.getWorkerService() != null) {
            Map<String, FunctionRuntimeInfo> functionRuntimes
                    = pulsar.getWorkerService().getFunctionRuntimeManager().getFunctionRuntimeInfos();
            String cluster = pulsar.getConfiguration().getClusterName();

            for (Map.Entry<String, FunctionRuntimeInfo> entry : functionRuntimes.entrySet()) {
                String fullyQualifiedInstanceName = entry.getKey();
                FunctionRuntimeInfo functionRuntimeInfo = entry.getValue();
                RuntimeSpawner functionRuntimeSpawner = functionRuntimeInfo.getRuntimeSpawner();

                if (functionRuntimeSpawner != null) {
                    Runtime functionRuntime = functionRuntimeSpawner.getRuntime();
                    if (functionRuntime != null) {
                        try {
                            InstanceCommunication.MetricsData metrics = functionRuntime.getAndResetMetrics().get();
                            for (Map.Entry<String, InstanceCommunication.MetricsData.DataDigest> metricsEntry
                                    : metrics.getMetricsMap().entrySet()) {
                                String metricName = metricsEntry.getKey();
                                InstanceCommunication.MetricsData.DataDigest dataDigest = metricsEntry.getValue();

                                String tenant = functionRuntimeInfo.getFunctionInstance()
                                        .getFunctionMetaData().getFunctionConfig().getTenant();
                                String namespace = functionRuntimeInfo.getFunctionInstance()
                                        .getFunctionMetaData().getFunctionConfig().getNamespace();
                                String name = functionRuntimeInfo.getFunctionInstance()
                                        .getFunctionMetaData().getFunctionConfig().getName();
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

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace,
                               String functionName, String metricName, int instanceId, double value) {
        stream.write(metricName).write("{cluster=\"").write(cluster).write("\", namespace=\"").write(namespace)
                .write("\", name=\"").write(functionName).write("\", instanceId=\"").write(instanceId).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }
}
