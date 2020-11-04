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

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.dlog.DLInputStream;
import org.apache.pulsar.functions.worker.dlog.DLOutputStream;
import org.apache.zookeeper.KeeperException.Code;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public final class WorkerUtils {

    private WorkerUtils(){}

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
            log.info("Target function file already exists at '{}'. Overwriting it now", destPkgPath);
            dlogNamespace.deleteLog(destPkgPath);
        }
        // copy the topology package to target working directory
        log.info("Uploading function package to '{}'", destPkgPath);

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

    public static void downloadFromBookkeeper(Namespace namespace,
                                              File outputFile,
                                              String packagePath) throws IOException {
        downloadFromBookkeeper(namespace, new FileOutputStream(outputFile), packagePath);
    }

    public static void downloadFromBookkeeper(Namespace namespace,
                                              OutputStream outputStream,
                                              String packagePath) throws IOException {
        log.info("Downloading {} from BK...", packagePath);
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

    public static void deleteFromBookkeeper(Namespace namespace, String packagePath) throws IOException {
        log.info("Deleting {} from BK", packagePath);
        namespace.deleteLog(packagePath);
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
        if (isNotBlank(workerConfig.getBookkeeperClientAuthenticationPlugin())) {
            conf.setProperty("bkc.clientAuthProviderFactoryClass",
                    workerConfig.getBookkeeperClientAuthenticationPlugin());
            if (isNotBlank(workerConfig.getBookkeeperClientAuthenticationParametersName())) {
                conf.setProperty("bkc." + workerConfig.getBookkeeperClientAuthenticationParametersName(),
                        workerConfig.getBookkeeperClientAuthenticationParameters());
            }
        }
        return conf;
    }

    public static URI initializeDlogNamespace(InternalConfigurationData internalConf) throws IOException {
        String zookeeperServers = internalConf.getZookeeperServers();
        String ledgersRootPath;
        String ledgersStoreServers;
        // for BC purposes
        if (internalConf.getBookkeeperMetadataServiceUri() == null) {
            ledgersRootPath = internalConf.getLedgersRootPath();
            ledgersStoreServers = zookeeperServers;
        } else {
            URI metadataServiceUri = URI.create(internalConf.getBookkeeperMetadataServiceUri());
            ledgersStoreServers = metadataServiceUri.getAuthority().replace(";", ",");
            ledgersRootPath = metadataServiceUri.getPath();
        }
        BKDLConfig dlConfig = new BKDLConfig(ledgersStoreServers, ledgersRootPath);
        DLMetadata dlMetadata = DLMetadata.create(dlConfig);
        URI dlogUri = URI.create(String.format("distributedlog://%s/pulsar/functions", zookeeperServers));

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

    public static PulsarAdmin getPulsarAdminClient(String pulsarWebServiceUrl) {
        return getPulsarAdminClient(pulsarWebServiceUrl, null, null, null, null, null);
    }

    public static PulsarAdmin getPulsarAdminClient(String pulsarWebServiceUrl, String authPlugin, String authParams,
                                                   String tlsTrustCertsFilePath, Boolean allowTlsInsecureConnection,
                                                   Boolean enableTlsHostnameVerificationEnable) {
        try {
            PulsarAdminBuilder adminBuilder = PulsarAdmin.builder().serviceHttpUrl(pulsarWebServiceUrl);
            if (isNotBlank(authPlugin) && isNotBlank(authParams)) {
                adminBuilder.authentication(authPlugin, authParams);
            }
            if (isNotBlank(tlsTrustCertsFilePath)) {
                adminBuilder.tlsTrustCertsFilePath(tlsTrustCertsFilePath);
            }
            if (allowTlsInsecureConnection != null) {
                adminBuilder.allowTlsInsecureConnection(allowTlsInsecureConnection);
            }
            if (enableTlsHostnameVerificationEnable != null) {
                adminBuilder.enableTlsHostnameVerification(enableTlsHostnameVerificationEnable);
            }
            return adminBuilder.build();
        } catch (PulsarClientException e) {
            log.error("Error creating pulsar admin client", e);
            throw new RuntimeException(e);
        }
    }

    public static PulsarClient getPulsarClient(String pulsarServiceUrl) {
        return getPulsarClient(pulsarServiceUrl, null, null, null,
                null, null, null);
    }

    public static PulsarClient getPulsarClient(String pulsarServiceUrl, String authPlugin, String authParams,
                                               Boolean useTls, String tlsTrustCertsFilePath,
                                               Boolean allowTlsInsecureConnection,
                                               Boolean enableTlsHostnameVerificationEnable) {

        try {
            ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(pulsarServiceUrl);

            if (isNotBlank(authPlugin)
                    && isNotBlank(authParams)) {
                clientBuilder.authentication(authPlugin, authParams);
            }
            if (useTls != null) {
                clientBuilder.enableTls(useTls);
            }
            if (allowTlsInsecureConnection != null) {
                clientBuilder.allowTlsInsecureConnection(allowTlsInsecureConnection);
            }
            if (isNotBlank(tlsTrustCertsFilePath)) {
                clientBuilder.tlsTrustCertsFilePath(tlsTrustCertsFilePath);
            }
            if (enableTlsHostnameVerificationEnable != null) {
                clientBuilder.enableTlsHostnameVerification(enableTlsHostnameVerificationEnable);
            }

            return clientBuilder.build();
        } catch (PulsarClientException e) {
            log.error("Error creating pulsar client", e);
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

    public static File dumpToTmpFile(final InputStream uploadedInputStream) {
        try {
            File tmpFile = FunctionCommon.createPkgTempFile();
            tmpFile.deleteOnExit();
            Files.copy(uploadedInputStream, tmpFile.toPath(), REPLACE_EXISTING);
            return tmpFile;
        } catch (IOException e) {
            throw new RuntimeException("Cannot create a temporary file", e);
        }
    }

    public static boolean isFunctionCodeBuiltin(Function.FunctionDetailsOrBuilder functionDetails) {
        if (functionDetails.hasSource()) {
            Function.SourceSpec sourceSpec = functionDetails.getSource();
            if (!StringUtils.isEmpty(sourceSpec.getBuiltin())) {
                return true;
            }
        }

        if (functionDetails.hasSink()) {
            Function.SinkSpec sinkSpec = functionDetails.getSink();
            if (!StringUtils.isEmpty(sinkSpec.getBuiltin())) {
                return true;
            }
        }

        if (!StringUtils.isEmpty(functionDetails.getBuiltin())) {
            return true;
        }

        return false;
    }

    public static Reader<byte[]> createReader(ReaderBuilder readerBuilder,
                                              String readerName,
                                              String topic,
                                              MessageId startMessageId) throws PulsarClientException {
        return readerBuilder
                .subscriptionRolePrefix(readerName)
                .readerName(readerName)
                .topic(topic)
                .readCompacted(true)
                .startMessageId(startMessageId)
                .create();
    }
}
