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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionDetailsOrBuilder;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.runtime.RuntimeUtils;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.common.functions.Utils.FILE;
import static org.apache.pulsar.common.functions.Utils.HTTP;
import static org.apache.pulsar.common.functions.Utils.isFunctionPackageUrlSupported;
import static org.apache.pulsar.functions.utils.Utils.getSinkType;
import static org.apache.pulsar.functions.utils.Utils.getSourceType;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Slf4j
public class FunctionActioner implements AutoCloseable {

    private final WorkerConfig workerConfig;
    private final RuntimeFactory runtimeFactory;
    private final Namespace dlogNamespace;
    private LinkedBlockingQueue<FunctionAction> actionQueue;
    private volatile boolean running;
    private Thread actioner;
    private final ConnectorsManager connectorsManager;
    private final PulsarAdmin pulsarAdmin;

    public FunctionActioner(WorkerConfig workerConfig,
                            RuntimeFactory runtimeFactory,
                            Namespace dlogNamespace,
                            LinkedBlockingQueue<FunctionAction> actionQueue,
                            ConnectorsManager connectorsManager, PulsarAdmin pulsarAdmin) {
        this.workerConfig = workerConfig;
        this.runtimeFactory = runtimeFactory;
        this.dlogNamespace = dlogNamespace;
        this.actionQueue = actionQueue;
        this.connectorsManager = connectorsManager;
        this.pulsarAdmin = pulsarAdmin;
        actioner = new Thread(() -> {
            log.info("Starting Actioner Thread...");
            while(running) {
                try {
                    FunctionAction action = actionQueue.poll(1, TimeUnit.SECONDS);
                    processAction(action);
                } catch (InterruptedException ex) {
                }
            }
        });
        actioner.setName("FunctionActionerThread");
    }


    void processAction(FunctionAction action) {
        if (action == null) return;

        switch (action.getAction()) {
            case START:
                try {
                    startFunction(action.getFunctionRuntimeInfo());
                } catch (Exception ex) {
                    FunctionDetails details = action.getFunctionRuntimeInfo().getFunctionInstance()
                            .getFunctionMetaData().getFunctionDetails();
                    log.info("{}/{}/{} Error starting function", details.getTenant(), details.getNamespace(),
                            details.getName(), ex);
                    action.getFunctionRuntimeInfo().setStartupException(ex);
                }
                break;
            case STOP:
                stopFunction(action.getFunctionRuntimeInfo());
                break;
            case TERMINATE:
                terminateFunction(action.getFunctionRuntimeInfo());
                break;
        }
    }

    public void start() {
        this.running = true;
        actioner.start();
    }

    @Override
    public void close() {
        running = false;
    }

    public void join() throws InterruptedException {
        actioner.join();
    }

    @VisibleForTesting
    public void startFunction(FunctionRuntimeInfo functionRuntimeInfo) throws Exception {
        FunctionMetaData functionMetaData = functionRuntimeInfo.getFunctionInstance().getFunctionMetaData();
        FunctionDetails functionDetails = functionMetaData.getFunctionDetails();
        int instanceId = functionRuntimeInfo.getFunctionInstance().getInstanceId();

        log.info("{}/{}/{}-{} Starting function ...", functionDetails.getTenant(), functionDetails.getNamespace(),
                functionDetails.getName(), instanceId);

        String packageFile;

        String pkgLocation = functionMetaData.getPackageLocation().getPackagePath();
        boolean isPkgUrlProvided = isFunctionPackageUrlSupported(pkgLocation);

        if (runtimeFactory.externallyManaged()) {
            packageFile = pkgLocation;
        } else {
            if (isPkgUrlProvided && pkgLocation.startsWith(FILE)) {
                URL url = new URL(pkgLocation);
                File pkgFile = new File(url.toURI());
                packageFile = pkgFile.getAbsolutePath();
            } else if (isFunctionCodeBuiltin(functionDetails)) {
                File pkgFile = getBuiltinArchive(FunctionDetails.newBuilder(functionMetaData.getFunctionDetails()));
                packageFile = pkgFile.getAbsolutePath();
            } else {
                File pkgDir = new File(workerConfig.getDownloadDirectory(),
                        getDownloadPackagePath(functionMetaData, instanceId));
                pkgDir.mkdirs();
                File pkgFile = new File(
                        pkgDir,
                        new File(FunctionDetailsUtils.getDownloadFileName(functionMetaData.getFunctionDetails(), functionMetaData.getPackageLocation())).getName());
                downloadFile(pkgFile, isPkgUrlProvided, functionMetaData, instanceId);
                packageFile = pkgFile.getAbsolutePath();
            }
        }

        RuntimeSpawner runtimeSpawner = getRuntimeSpawner(functionRuntimeInfo.getFunctionInstance(), packageFile);
        functionRuntimeInfo.setRuntimeSpawner(runtimeSpawner);

        runtimeSpawner.start();
    }

    RuntimeSpawner getRuntimeSpawner(Function.Instance instance, String packageFile) {
        FunctionMetaData functionMetaData = instance.getFunctionMetaData();
        int instanceId = instance.getInstanceId();

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder(functionMetaData.getFunctionDetails());

        InstanceConfig instanceConfig = createInstanceConfig(functionDetailsBuilder.build(),
                instanceId, workerConfig.getPulsarFunctionsCluster());

        RuntimeSpawner runtimeSpawner = new RuntimeSpawner(instanceConfig, packageFile,
                functionMetaData.getPackageLocation().getOriginalFileName(),
                runtimeFactory, workerConfig.getInstanceLivenessCheckFreqMs());

        return runtimeSpawner;
    }


    InstanceConfig createInstanceConfig(FunctionDetails functionDetails, int instanceId, String clusterName) {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetails);
        // TODO: set correct function id and version when features implemented
        instanceConfig.setFunctionId(UUID.randomUUID().toString());
        instanceConfig.setFunctionVersion(UUID.randomUUID().toString());
        instanceConfig.setInstanceId(instanceId);
        instanceConfig.setMaxBufferedTuples(1024);
        instanceConfig.setPort(org.apache.pulsar.functions.utils.Utils.findAvailablePort());
        instanceConfig.setClusterName(clusterName);
        return instanceConfig;
    }

    private void downloadFile(File pkgFile, boolean isPkgUrlProvided, FunctionMetaData functionMetaData, int instanceId) throws FileNotFoundException, IOException {

        FunctionDetails details = functionMetaData.getFunctionDetails();
        File pkgDir = pkgFile.getParentFile();

        if (pkgFile.exists()) {
            log.warn("Function package exists already {} deleting it",
                    pkgFile);
            pkgFile.delete();
        }

        File tempPkgFile;
        while (true) {
            tempPkgFile = new File(
                    pkgDir,
                    pkgFile.getName() + "." + instanceId + "." + UUID.randomUUID().toString());
            if (!tempPkgFile.exists() && tempPkgFile.createNewFile()) {
                break;
            }
        }
        String pkgLocationPath = functionMetaData.getPackageLocation().getPackagePath();
        boolean downloadFromHttp = isPkgUrlProvided && pkgLocationPath.startsWith(HTTP);
        log.info("{}/{}/{} Function package file {} will be downloaded from {}", tempPkgFile, details.getTenant(),
                details.getNamespace(), details.getName(),
                downloadFromHttp ? pkgLocationPath : functionMetaData.getPackageLocation());

        if(downloadFromHttp) {
            Utils.downloadFromHttpUrl(pkgLocationPath, new FileOutputStream(tempPkgFile));
        } else {
            Utils.downloadFromBookkeeper(
                    dlogNamespace,
                    new FileOutputStream(tempPkgFile),
                    pkgLocationPath);
        }

        try {
            // create a hardlink, if there are two concurrent createLink operations, one will fail.
            // this ensures one instance will successfully download the package.
            try {
                Files.createLink(
                        Paths.get(pkgFile.toURI()),
                        Paths.get(tempPkgFile.toURI()));
                log.info("Function package file is linked from {} to {}",
                        tempPkgFile, pkgFile);
            } catch (FileAlreadyExistsException faee) {
                // file already exists
                log.warn("Function package has been downloaded from {} and saved at {}",
                        functionMetaData.getPackageLocation(), pkgFile);
            }
        } finally {
            tempPkgFile.delete();
        }
    }

    public void stopFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        Function.Instance instance = functionRuntimeInfo.getFunctionInstance();
        FunctionMetaData functionMetaData = instance.getFunctionMetaData();
        FunctionDetails details = functionMetaData.getFunctionDetails();
        log.info("{}/{}/{}-{} Stopping function...", details.getTenant(), details.getNamespace(), details.getName(),
                instance.getInstanceId());
        if (functionRuntimeInfo.getRuntimeSpawner() != null) {
            functionRuntimeInfo.getRuntimeSpawner().close();
            functionRuntimeInfo.setRuntimeSpawner(null);
        }

        // clean up function package
        File pkgDir = new File(
                workerConfig.getDownloadDirectory(),
                getDownloadPackagePath(functionMetaData, instance.getInstanceId()));

        if (pkgDir.exists()) {
            try {
                MoreFiles.deleteRecursively(
                    Paths.get(pkgDir.toURI()), RecursiveDeleteOption.ALLOW_INSECURE);
            } catch (IOException e) {
                log.warn("Failed to delete package for function: {}",
                        FunctionDetailsUtils.getFullyQualifiedName(functionMetaData.getFunctionDetails()), e);
            }
        }
    }

    private void terminateFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        FunctionDetails details = functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails();
        String fqfn = FunctionDetailsUtils.getFullyQualifiedName(details);

        log.info("{}-{} Terminating function...", fqfn,functionRuntimeInfo.getFunctionInstance().getInstanceId());

        stopFunction(functionRuntimeInfo);
        //cleanup subscriptions
        if (details.getSource().getCleanupSubscription()) {
            Map<String, Function.ConsumerSpec> consumerSpecMap = details.getSource().getInputSpecsMap();
            consumerSpecMap.entrySet().forEach(new Consumer<Map.Entry<String, Function.ConsumerSpec>>() {
                @Override
                public void accept(Map.Entry<String, Function.ConsumerSpec> stringConsumerSpecEntry) {

                    Function.ConsumerSpec consumerSpec = stringConsumerSpecEntry.getValue();
                    String topic = stringConsumerSpecEntry.getKey();

                    String subscriptionName = isBlank(functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails().getSource().getSubscriptionName())
                            ? InstanceUtils.getDefaultSubscriptionName(functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails())
                            : functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails().getSource().getSubscriptionName();

                    try {
                        RuntimeUtils.Actions.newBuilder()
                                .addAction(
                                        RuntimeUtils.Actions.Action.builder()
                                                .actionName(String.format("Cleaning up subscriptions for function %s", fqfn))
                                                .numRetries(10)
                                                .sleepBetweenInvocationsMs(1000)
                                                .supplier(() -> {
                                                    try {
                                                        if (consumerSpec.getIsRegexPattern()) {
                                                            pulsarAdmin.namespaces().unsubscribeNamespace(TopicName
                                                                    .get(topic).getNamespace(), subscriptionName);
                                                        } else {
                                                            pulsarAdmin.topics().deleteSubscription(topic,
                                                                    subscriptionName);
                                                        }
                                                    } catch (PulsarAdminException e) {
                                                        if (e instanceof PulsarAdminException.NotFoundException) {
                                                            return RuntimeUtils.Actions.ActionResult.builder()
                                                                    .success(true)
                                                                    .build();
                                                        } else {
                                                            // for debugging purposes
                                                            List<Map<String, String>> existingConsumers = Collections.emptyList();
                                                            try {
                                                                TopicStats stats = pulsarAdmin.topics().getStats(topic);
                                                                SubscriptionStats sub = stats.subscriptions.get(InstanceUtils.getDefaultSubscriptionName(details));
                                                                if (sub != null) {
                                                                    existingConsumers = sub.consumers.stream()
                                                                    .map(consumerStats -> consumerStats.metadata)
                                                                    .collect(Collectors.toList());
                                                                }
                                                            } catch (PulsarAdminException e1) {

                                                            }

                                                            String errorMsg = e.getHttpError() != null ? e.getHttpError() : e.getMessage();
                                                            return RuntimeUtils.Actions.ActionResult.builder()
                                                                    .success(false)
                                                                    .errorMsg(String.format("%s - existing consumers: %s", errorMsg, existingConsumers))
                                                                    .build();
                                                        }
                                                    }

                                                    return RuntimeUtils.Actions.ActionResult.builder()
                                                            .success(true)
                                                            .build();

                                                })
                                                .build())
                                .run();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    private String getDownloadPackagePath(FunctionMetaData functionMetaData, int instanceId) {
        return StringUtils.join(
                new String[]{
                        functionMetaData.getFunctionDetails().getTenant(),
                        functionMetaData.getFunctionDetails().getNamespace(),
                        functionMetaData.getFunctionDetails().getName(),
                        Integer.toString(instanceId),
                },
                File.separatorChar);
    }

    public static boolean isFunctionCodeBuiltin(FunctionDetailsOrBuilder functionDetails) {
        if (functionDetails.hasSource()) {
            SourceSpec sourceSpec = functionDetails.getSource();
            if (!StringUtils.isEmpty(sourceSpec.getBuiltin())) {
                return true;
            }
        }

        if (functionDetails.hasSink()) {
            SinkSpec sinkSpec = functionDetails.getSink();
            if (!StringUtils.isEmpty(sinkSpec.getBuiltin())) {
                return true;
            }
        }

        return false;
    }

    private File getBuiltinArchive(FunctionDetails.Builder functionDetails) throws IOException {
        if (functionDetails.hasSource()) {
            SourceSpec sourceSpec = functionDetails.getSource();
            if (!StringUtils.isEmpty(sourceSpec.getBuiltin())) {
                File archive = connectorsManager.getSourceArchive(sourceSpec.getBuiltin()).toFile();
                String sourceClass = ConnectorUtils.getConnectorDefinition(archive.toString()).getSourceClass();
                SourceSpec.Builder builder = SourceSpec.newBuilder(functionDetails.getSource());
                builder.setClassName(sourceClass);
                functionDetails.setSource(builder);

                fillSourceTypeClass(functionDetails, archive, sourceClass);
                return archive;
            }
        }

        if (functionDetails.hasSink()) {
            SinkSpec sinkSpec = functionDetails.getSink();
            if (!StringUtils.isEmpty(sinkSpec.getBuiltin())) {
                File archive = connectorsManager.getSinkArchive(sinkSpec.getBuiltin()).toFile();
                String sinkClass = ConnectorUtils.getConnectorDefinition(archive.toString()).getSinkClass();
                SinkSpec.Builder builder = SinkSpec.newBuilder(functionDetails.getSink());
                builder.setClassName(sinkClass);
                functionDetails.setSink(builder);

                fillSinkTypeClass(functionDetails, archive, sinkClass);
                return archive;
            }
        }

        throw new IOException("Could not find built in archive definition");
    }

    private void fillSourceTypeClass(FunctionDetails.Builder functionDetails, File archive, String className)
            throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(archive, Collections.emptySet())) {
            String typeArg = getSourceType(className, ncl).getName();

            SourceSpec.Builder sourceBuilder = SourceSpec.newBuilder(functionDetails.getSource());
            sourceBuilder.setTypeClassName(typeArg);
            functionDetails.setSource(sourceBuilder);

            SinkSpec sinkSpec = functionDetails.getSink();
            if (null == sinkSpec || StringUtils.isEmpty(sinkSpec.getTypeClassName())) {
                SinkSpec.Builder sinkBuilder = SinkSpec.newBuilder(sinkSpec);
                sinkBuilder.setTypeClassName(typeArg);
                functionDetails.setSink(sinkBuilder);
            }
        }
    }

    private void fillSinkTypeClass(FunctionDetails.Builder functionDetails, File archive, String className)
            throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(archive, Collections.emptySet())) {
            String typeArg = getSinkType(className, ncl).getName();

            SinkSpec.Builder sinkBuilder = SinkSpec.newBuilder(functionDetails.getSink());
            sinkBuilder.setTypeClassName(typeArg);
            functionDetails.setSink(sinkBuilder);

            SourceSpec sourceSpec = functionDetails.getSource();
            if (null == sourceSpec || StringUtils.isEmpty(sourceSpec.getTypeClassName())) {
                SourceSpec.Builder sourceBuilder = SourceSpec.newBuilder(sourceSpec);
                sourceBuilder.setTypeClassName(typeArg);
                functionDetails.setSource(sourceBuilder);
            }
        }
    }

}