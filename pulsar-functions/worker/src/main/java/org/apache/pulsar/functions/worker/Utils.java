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

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.worker.dlog.DLInputStream;
import org.apache.pulsar.functions.worker.dlog.DLOutputStream;
import org.apache.zookeeper.KeeperException.Code;

@Slf4j
public final class Utils {

    private Utils(){}

    public static byte[] toByteArray(Object obj) throws IOException {
        byte[] bytes = null;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
        }
        return bytes;
    }

    public static String getUniquePackageName(String packageName) {
        return String.format("%s-%s", UUID.randomUUID().toString(), packageName);
    }

    public static void uploadFileToBookkeeper(String packagePath, File sourceFile, Namespace dlogNamespace) throws IOException {
        FileInputStream uploadedInputStream = new FileInputStream(sourceFile);
        uploadToBookeeper(dlogNamespace, uploadedInputStream, packagePath);
    }

    public static void uploadToBookeeper(Namespace dlogNamespace,
                                         InputStream uploadedInputStream,
                                         String destPkgPath)
            throws IOException {

        // if the dest directory does not exist, create it.
        if (dlogNamespace.logExists(destPkgPath)) {
            // if the destination file exists, write a log message
            log.info(String.format("Target function file already exists at '%s'. Overwriting it now",
                    destPkgPath));
            dlogNamespace.deleteLog(destPkgPath);
        }
        // copy the topology package to target working directory
        log.info(String.format("Uploading function package to '%s'",
                destPkgPath));

        try (DistributedLogManager dlm = dlogNamespace.openLog(destPkgPath)) {
            try (AppendOnlyStreamWriter writer = dlm.getAppendOnlyStreamWriter()){

                try (OutputStream out = new DLOutputStream(dlm, writer)) {
                    int read = 0;
                    byte[] bytes = new byte[1024];
                    while ((read = uploadedInputStream.read(bytes)) != -1) {
                        out.write(bytes, 0, read);
                    }
                    out.flush();
                }
            }
        }
    }
    
    public static void downloadFromHttpUrl(String destPkgUrl, FileOutputStream outputStream) throws IOException {
        URL website = new URL(destPkgUrl);
        ReadableByteChannel rbc = Channels.newChannel(website.openStream());
        outputStream.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
    }

    public static void downloadFromBookkeeper(Namespace namespace,
                                              File outputFile,
                                              String packagePath) throws IOException {
        downloadFromBookkeeper(namespace, new FileOutputStream(outputFile), packagePath);
    }

    public static void downloadFromBookkeeper(Namespace namespace,
                                              OutputStream outputStream,
                                              String packagePath) throws IOException {
        DistributedLogManager dlm = namespace.openLog(packagePath);
        try (InputStream in = new DLInputStream(dlm)) {
            int read = 0;
            byte[] bytes = new byte[1024];
            while ((read = in.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }
            outputStream.flush();
        }
    }

    public static DistributedLogConfiguration getDlogConf(WorkerConfig workerConfig) {
        int numReplicas = workerConfig.getNumFunctionPackageReplicas();

        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setWriteLockEnabled(false)
                .setOutputBufferSize(256 * 1024)                  // 256k
                .setPeriodicFlushFrequencyMilliSeconds(0)         // disable periodical flush
                .setImmediateFlushEnabled(false)                  // disable immediate flush
                .setLogSegmentRollingIntervalMinutes(0)           // disable time-based rolling
                .setMaxLogSegmentBytes(Long.MAX_VALUE)            // disable size-based rolling
                .setExplicitTruncationByApplication(true)         // no auto-truncation
                .setRetentionPeriodHours(Integer.MAX_VALUE)       // long retention
                .setEnsembleSize(numReplicas)                     // replica settings
                .setWriteQuorumSize(numReplicas)
                .setAckQuorumSize(numReplicas)
                .setUseDaemonThread(true);
        conf.setProperty("bkc.allowShadedLedgerManagerFactoryClass", true);
        conf.setProperty("bkc.shadedLedgerManagerFactoryClassPrefix", "dlshade.");
        return conf;
    }

    public static URI initializeDlogNamespace(String zkServers, String ledgersRootPath) throws IOException {
        BKDLConfig dlConfig = new BKDLConfig(zkServers, ledgersRootPath);
        DLMetadata dlMetadata = DLMetadata.create(dlConfig);
        URI dlogUri = URI.create(String.format("distributedlog://%s/pulsar/functions", zkServers));

        try {
            dlMetadata.create(dlogUri);
        } catch (ZKException e) {
            if (e.getKeeperExceptionCode() == Code.NODEEXISTS) {
                return dlogUri;
            }
            throw e;
        }
        return dlogUri;
    }

    public static PulsarAdmin getPulsarAdminClient(String pulsarWebServiceUrl, String authPlugin, String authParams, String tlsTrustCertsFilePath, boolean allowTlsInsecureConnection) {
        try {
            PulsarAdminBuilder adminBuilder = PulsarAdmin.builder().serviceHttpUrl(pulsarWebServiceUrl);
            if (isNotBlank(authPlugin) && isNotBlank(authParams)) {
                adminBuilder.authentication(authPlugin, authParams);
            }
            if (isNotBlank(tlsTrustCertsFilePath)) {
                adminBuilder.tlsTrustCertsFilePath(tlsTrustCertsFilePath);
            }
            adminBuilder.allowTlsInsecureConnection(allowTlsInsecureConnection);
            return adminBuilder.build();
        } catch (PulsarClientException e) {
            log.error("Error creating pulsar admin client", e);
            throw new RuntimeException(e);
        }
    }

    public static FunctionStats.FunctionInstanceStats getFunctionInstanceStats(String fullyQualifiedInstanceName,
                                                                               FunctionRuntimeInfo functionRuntimeInfo,
                                                                               int instanceId) {
        RuntimeSpawner functionRuntimeSpawner = functionRuntimeInfo.getRuntimeSpawner();

        FunctionStats.FunctionInstanceStats functionInstanceStats = new FunctionStats.FunctionInstanceStats();
        if (functionRuntimeSpawner != null) {
            Runtime functionRuntime = functionRuntimeSpawner.getRuntime();
            if (functionRuntime != null) {
                try {

                    InstanceCommunication.MetricsData metricsData = functionRuntime.getMetrics(instanceId).get();
                    functionInstanceStats.setInstanceId(instanceId);

                    FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStatsData
                            = new FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData();

                    functionInstanceStatsData.setReceivedTotal(metricsData.getReceivedTotal());
                    functionInstanceStatsData.setProcessedSuccessfullyTotal(metricsData.getProcessedSuccessfullyTotal());
                    functionInstanceStatsData.setSystemExceptionsTotal(metricsData.getSystemExceptionsTotal());
                    functionInstanceStatsData.setUserExceptionsTotal(metricsData.getUserExceptionsTotal());
                    functionInstanceStatsData.setAvgProcessLatency(metricsData.getAvgProcessLatency() == 0.0 ? null : metricsData.getAvgProcessLatency());
                    functionInstanceStatsData.setLastInvocation(metricsData.getLastInvocation() == 0 ? null : metricsData.getLastInvocation());

                    functionInstanceStatsData.oneMin.setReceivedTotal(metricsData.getReceivedTotal1Min());
                    functionInstanceStatsData.oneMin.setProcessedSuccessfullyTotal(metricsData.getProcessedSuccessfullyTotal1Min());
                    functionInstanceStatsData.oneMin.setSystemExceptionsTotal(metricsData.getSystemExceptionsTotal1Min());
                    functionInstanceStatsData.oneMin.setUserExceptionsTotal(metricsData.getUserExceptionsTotal1Min());
                    functionInstanceStatsData.oneMin.setAvgProcessLatency(metricsData.getAvgProcessLatency1Min() == 0.0 ? null : metricsData.getAvgProcessLatency1Min());

                    // Filter out values that are NaN
                    Map<String, Double> statsDataMap = metricsData.getUserMetricsMap().entrySet().stream()
                            .filter(stringDoubleEntry -> !stringDoubleEntry.getValue().isNaN())
                            .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));

                    functionInstanceStatsData.setUserMetrics(statsDataMap);

                    functionInstanceStats.setMetrics(functionInstanceStatsData);
                } catch (InterruptedException | ExecutionException e) {
                    log.warn("Failed to collect metrics for function instance {}", fullyQualifiedInstanceName, e);
                }
            }
        }
        return functionInstanceStats;
    }
}
