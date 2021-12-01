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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.io.BatchSourceConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.utils.Actions;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.io.Connector;

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
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.common.functions.Utils.FILE;
import static org.apache.pulsar.common.functions.Utils.HTTP;
import static org.apache.pulsar.common.functions.Utils.isFunctionPackageUrlSupported;
import static org.apache.pulsar.functions.auth.FunctionAuthUtils.getFunctionAuthData;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSinkType;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSourceType;

@Data
@Slf4j
public class FunctionActioner {

    private final WorkerConfig workerConfig;
    private final RuntimeFactory runtimeFactory;
    private final Namespace dlogNamespace;
    private final ConnectorsManager connectorsManager;
    private final FunctionsManager functionsManager;
    private final PulsarAdmin pulsarAdmin;

    public FunctionActioner(WorkerConfig workerConfig,
                            RuntimeFactory runtimeFactory,
                            Namespace dlogNamespace,
                            ConnectorsManager connectorsManager,FunctionsManager functionsManager,PulsarAdmin pulsarAdmin) {
        this.workerConfig = workerConfig;
        this.runtimeFactory = runtimeFactory;
        this.dlogNamespace = dlogNamespace;
        this.connectorsManager = connectorsManager;
        this.functionsManager = functionsManager;
        this.pulsarAdmin = pulsarAdmin;
    }


    public void startFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        try {
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
                } else if (FunctionCommon.isFunctionCodeBuiltin(functionDetails)) {
                    File pkgFile = getBuiltinArchive(FunctionDetails.newBuilder(functionMetaData.getFunctionDetails()));
                    packageFile = pkgFile.getAbsolutePath();
                } else {
                    File pkgDir = new File(workerConfig.getDownloadDirectory(),
                            getDownloadPackagePath(functionMetaData, instanceId));
                    pkgDir.mkdirs();
                    File pkgFile = new File(
                            pkgDir,
                            new File(getDownloadFileName(functionMetaData.getFunctionDetails(), functionMetaData.getPackageLocation())).getName());
                    downloadFile(pkgFile, isPkgUrlProvided, functionMetaData, instanceId);
                    packageFile = pkgFile.getAbsolutePath();
                }
            }

            // Setup for batch sources if necessary
            setupBatchSource(functionDetails);

            RuntimeSpawner runtimeSpawner = getRuntimeSpawner(functionRuntimeInfo.getFunctionInstance(), packageFile);
            functionRuntimeInfo.setRuntimeSpawner(runtimeSpawner);

            runtimeSpawner.start();
        } catch (Exception ex) {
            FunctionDetails details = functionRuntimeInfo.getFunctionInstance()
                    .getFunctionMetaData().getFunctionDetails();
            log.error("{}/{}/{} Error starting function", details.getTenant(), details.getNamespace(),
                    details.getName(), ex);
            functionRuntimeInfo.setStartupException(ex);
        }
    }

    RuntimeSpawner getRuntimeSpawner(Function.Instance instance, String packageFile) {
        FunctionMetaData functionMetaData = instance.getFunctionMetaData();
        int instanceId = instance.getInstanceId();

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder(functionMetaData.getFunctionDetails());

        // check to make sure functionAuthenticationSpec has any data and authentication is enabled.
        // If not set to null, since for protobuf,
        // even if the field is not set its not going to be null. Have to use the "has" method to check
        Function.FunctionAuthenticationSpec functionAuthenticationSpec = null;
        if (workerConfig.isAuthenticationEnabled() && instance.getFunctionMetaData().hasFunctionAuthSpec()) {
            functionAuthenticationSpec = instance.getFunctionMetaData().getFunctionAuthSpec();
        }

        InstanceConfig instanceConfig = createInstanceConfig(functionDetailsBuilder.build(),
                functionAuthenticationSpec,
                instanceId, workerConfig.getPulsarFunctionsCluster());

        RuntimeSpawner runtimeSpawner = new RuntimeSpawner(instanceConfig, packageFile,
                functionMetaData.getPackageLocation().getOriginalFileName(),
                runtimeFactory, workerConfig.getInstanceLivenessCheckFreqMs());

        return runtimeSpawner;
    }

    InstanceConfig createInstanceConfig(FunctionDetails functionDetails, Function.FunctionAuthenticationSpec
            functionAuthSpec, int instanceId, String clusterName) {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetails);
        // TODO: set correct function id and version when features implemented
        instanceConfig.setFunctionId(UUID.randomUUID().toString());
        instanceConfig.setFunctionVersion(UUID.randomUUID().toString());
        instanceConfig.setInstanceId(instanceId);
        instanceConfig.setMaxBufferedTuples(1024);
        instanceConfig.setPort(FunctionCommon.findAvailablePort());
        instanceConfig.setMetricsPort(FunctionCommon.findAvailablePort());
        instanceConfig.setClusterName(clusterName);
        instanceConfig.setFunctionAuthenticationSpec(functionAuthSpec);
        instanceConfig.setMaxPendingAsyncRequests(workerConfig.getMaxPendingAsyncRequests());
        instanceConfig.setExposePulsarAdminClientEnabled(workerConfig.isExposeAdminClientEnabled());
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
        do {
            tempPkgFile = new File(
                    pkgDir,
                    pkgFile.getName() + "." + instanceId + "." + UUID.randomUUID().toString());
        } while (tempPkgFile.exists() || !tempPkgFile.createNewFile());
        String pkgLocationPath = functionMetaData.getPackageLocation().getPackagePath();
        boolean downloadFromHttp = isPkgUrlProvided && pkgLocationPath.startsWith(HTTP);
        log.info("{}/{}/{} Function package file {} will be downloaded from {}", tempPkgFile, details.getTenant(),
                details.getNamespace(), details.getName(),
                downloadFromHttp ? pkgLocationPath : functionMetaData.getPackageLocation());

        if(downloadFromHttp) {
            FunctionCommon.downloadFromHttpUrl(pkgLocationPath, tempPkgFile);
        } else {
            FileOutputStream tempPkgFos = new FileOutputStream(tempPkgFile);
            WorkerUtils.downloadFromBookkeeper(
                    dlogNamespace,
                    tempPkgFos,
                    pkgLocationPath);
            if (tempPkgFos != null) {
                tempPkgFos.close();
            }
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

        if(details.getRuntime() == Function.FunctionDetails.Runtime.GO && !pkgFile.canExecute()) {
            pkgFile.setExecutable(true);
            log.info("Golang function package file {} is set to executable", pkgFile);
        }
    }

    private void cleanupFunctionFiles(FunctionRuntimeInfo functionRuntimeInfo) {
        Function.Instance instance = functionRuntimeInfo.getFunctionInstance();
        FunctionMetaData functionMetaData = instance.getFunctionMetaData();
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
                        FunctionCommon.getFullyQualifiedName(functionMetaData.getFunctionDetails()), e);
            }
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

        cleanupFunctionFiles(functionRuntimeInfo);
    }

    public void terminateFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        FunctionDetails details = functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails();
        String fqfn = FunctionCommon.getFullyQualifiedName(details);
        log.info("{}-{} Terminating function...", fqfn,functionRuntimeInfo.getFunctionInstance().getInstanceId());

        if (functionRuntimeInfo.getRuntimeSpawner() != null) {
            functionRuntimeInfo.getRuntimeSpawner().close();

            // cleanup any auth data cached
            if (workerConfig.isAuthenticationEnabled()) {
                functionRuntimeInfo.getRuntimeSpawner()
                        .getRuntimeFactory().getAuthProvider().ifPresent(functionAuthProvider -> {
                            try {
                                log.info("{}-{} Cleaning up authentication data for function...", fqfn,functionRuntimeInfo.getFunctionInstance().getInstanceId());
                                functionAuthProvider
                                        .cleanUpAuthData(
                                                details,
                                                Optional.ofNullable(getFunctionAuthData(
                                                        Optional.ofNullable(
                                                                functionRuntimeInfo.getRuntimeSpawner().getInstanceConfig().getFunctionAuthenticationSpec()))));

                            } catch (Exception e) {
                                log.error("Failed to cleanup auth data for function: {}", fqfn, e);
                            }
                        });
            }
            functionRuntimeInfo.setRuntimeSpawner(null);
        }

        cleanupFunctionFiles(functionRuntimeInfo);

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

                    deleteSubscription(topic, consumerSpec, subscriptionName, String.format("Cleaning up subscriptions for function %s", fqfn));
                }
            });
        }

        // clean up done for batch sources if necessary
        cleanupBatchSource(details);
    }

    private void deleteSubscription(String topic, Function.ConsumerSpec consumerSpec, String subscriptionName, String msg) {
        try {
            Actions.newBuilder()
                    .addAction(
                      Actions.Action.builder()
                        .actionName(msg)
                        .numRetries(10)
                        .sleepBetweenInvocationsMs(1000)
                        .supplier(
                          getDeleteSubscriptionSupplier(topic,
                            consumerSpec.getIsRegexPattern(),
                            subscriptionName)
                        )
                        .build())
                    .run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Supplier<Actions.ActionResult> getDeleteSubscriptionSupplier(
      String topic, boolean isRegex, String subscriptionName) {
        return () -> {
            try {
                if (isRegex) {
                    pulsarAdmin.namespaces().unsubscribeNamespace(TopicName
                      .get(topic).getNamespace(), subscriptionName);
                } else {
                    pulsarAdmin.topics().deleteSubscription(topic,
                      subscriptionName);
                }
            } catch (PulsarAdminException e) {
                if (e instanceof PulsarAdminException.NotFoundException) {
                    return Actions.ActionResult.builder()
                      .success(true)
                      .build();
                } else {
                    // for debugging purposes
                    List<Map<String, String>> existingConsumers = Collections.emptyList();
                    SubscriptionStats sub = null;
                    try {
                        TopicStats stats = pulsarAdmin.topics().getStats(topic);
                        sub = stats.getSubscriptions().get(subscriptionName);
                        if (sub != null) {
                            existingConsumers = sub.getConsumers().stream()
                              .map(consumerStats -> consumerStats.getMetadata())
                              .collect(Collectors.toList());
                        }
                    } catch (PulsarAdminException e1) {

                    }

                    String errorMsg = e.getHttpError() != null ? e.getHttpError() : e.getMessage();
                    String finalErrorMsg;
                    if (sub != null) {
                        try {
                            finalErrorMsg = String.format("%s - existing consumers: %s",
                              errorMsg, ObjectMapperFactory.getThreadLocal().writeValueAsString(sub));
                        } catch (JsonProcessingException jsonProcessingException) {
                            finalErrorMsg = errorMsg;
                        }
                    } else {
                        finalErrorMsg = errorMsg;
                    }
                    return Actions.ActionResult.builder()
                      .success(false)
                      .errorMsg(finalErrorMsg)
                      .build();
                }
            }

            return Actions.ActionResult.builder()
              .success(true)
              .build();
        };
    }

    private Supplier<Actions.ActionResult> getDeleteTopicSupplier(String topic) {
        return () -> {
            try {
                pulsarAdmin.topics().delete(topic, true);
            } catch (PulsarAdminException e) {
                if (e instanceof PulsarAdminException.NotFoundException) {
                    return Actions.ActionResult.builder()
                      .success(true)
                      .build();
                } else {
                    // for debugging purposes
                    TopicStats stats = null;
                    try {
                        stats = pulsarAdmin.topics().getStats(topic);
                    } catch (PulsarAdminException e1) {

                    }

                    String errorMsg = e.getHttpError() != null ? e.getHttpError() : e.getMessage();
                    String finalErrorMsg;
                    if (stats != null) {
                        try {
                            finalErrorMsg = String.format("%s - topic stats: %s",
                              errorMsg, ObjectMapperFactory.getThreadLocal().writeValueAsString(stats));
                        } catch (JsonProcessingException jsonProcessingException) {
                            finalErrorMsg = errorMsg;
                        }
                    } else {
                        finalErrorMsg = errorMsg;
                    }

                    return Actions.ActionResult.builder()
                      .success(false)
                      .errorMsg(finalErrorMsg)
                      .build();
                }
            }

            return Actions.ActionResult.builder()
              .success(true)
              .build();
        };
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

    private File getBuiltinArchive(FunctionDetails.Builder functionDetails) throws IOException, ClassNotFoundException {
        if (functionDetails.hasSource()) {
            SourceSpec sourceSpec = functionDetails.getSource();
            if (!StringUtils.isEmpty(sourceSpec.getBuiltin())) {
                Connector connector = connectorsManager.getConnector(sourceSpec.getBuiltin());
                File archive = connector.getArchivePath().toFile();
                String sourceClass = connector.getConnectorDefinition().getSourceClass();
                SourceSpec.Builder builder = SourceSpec.newBuilder(functionDetails.getSource());
                builder.setClassName(sourceClass);
                functionDetails.setSource(builder);

                fillSourceTypeClass(functionDetails, connector.getClassLoader(), sourceClass);
                return archive;
            }
        }

        if (functionDetails.hasSink()) {
            SinkSpec sinkSpec = functionDetails.getSink();
            if (!StringUtils.isEmpty(sinkSpec.getBuiltin())) {
                Connector connector = connectorsManager.getConnector(sinkSpec.getBuiltin());

                File archive = connector.getArchivePath().toFile();
                String sinkClass = connector.getConnectorDefinition().getSinkClass();
                SinkSpec.Builder builder = SinkSpec.newBuilder(functionDetails.getSink());
                builder.setClassName(sinkClass);
                functionDetails.setSink(builder);

                fillSinkTypeClass(functionDetails, connector.getClassLoader(), sinkClass);
                return archive;
            }
        }

        if (!StringUtils.isEmpty(functionDetails.getBuiltin())) {
            return functionsManager.getFunctionArchive(functionDetails.getBuiltin()).toFile();
        }

        throw new IOException("Could not find built in archive definition");
    }

    private void fillSourceTypeClass(FunctionDetails.Builder functionDetails,
                                     ClassLoader narClassLoader, String className) throws ClassNotFoundException {
        String typeArg = getSourceType(className, narClassLoader).getName();

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

    private void fillSinkTypeClass(FunctionDetails.Builder functionDetails,
                                   ClassLoader narClassLoader, String className) throws ClassNotFoundException {
        String typeArg = getSinkType(className, narClassLoader).getName();

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

    private static String getDownloadFileName(FunctionDetails FunctionDetails,
                                             Function.PackageLocationMetaData packageLocation) {
        if (!org.apache.commons.lang.StringUtils.isEmpty(packageLocation.getOriginalFileName())) {
            return packageLocation.getOriginalFileName();
        }
        String[] hierarchy = FunctionDetails.getClassName().split("\\.");
        String fileName;
        if (hierarchy.length <= 0) {
            fileName = FunctionDetails.getClassName();
        } else if (hierarchy.length == 1) {
            fileName =  hierarchy[0];
        } else {
            fileName = hierarchy[hierarchy.length - 2];
        }
        switch (FunctionDetails.getRuntime()) {
            case JAVA:
                return fileName + ".jar";
            case PYTHON:
                return fileName + ".py";
            case GO:
                return fileName + ".go";
            default:
                throw new RuntimeException("Unknown runtime " + FunctionDetails.getRuntime());
        }
    }

    private void setupBatchSource(Function.FunctionDetails functionDetails) {
        if (isBatchSource(functionDetails)) {

            String intermediateTopicName = SourceConfigUtils.computeBatchSourceIntermediateTopicName(
              functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName()).toString();

            String intermediateTopicSubscription = SourceConfigUtils.computeBatchSourceInstanceSubscriptionName(
              functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName());
            String fqfn = FunctionCommon.getFullyQualifiedName(
              functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName());
            try {
                Actions.newBuilder()
                  .addAction(
                    Actions.Action.builder()
                      .actionName(String.format("Creating intermediate topic %s with subscription %s for Batch Source %s",
                        intermediateTopicName, intermediateTopicSubscription, fqfn))
                      .numRetries(10)
                      .sleepBetweenInvocationsMs(1000)
                      .supplier(() -> {
                          try {
                              pulsarAdmin.topics().createSubscription(intermediateTopicName, intermediateTopicSubscription, MessageId.latest);
                              return Actions.ActionResult.builder()
                                .success(true)
                                .build();
                          } catch (PulsarAdminException.ConflictException e) {
                              // topic and subscription already exists so just continue
                              return Actions.ActionResult.builder()
                                .success(true)
                                .build();
                          } catch (Exception e) {
                              return Actions.ActionResult.builder()
                                .errorMsg(e.getMessage())
                                .success(false)
                                .build();
                          }
                      })
                      .build())
                  .run();
            } catch (InterruptedException e) {
                log.error("Error setting up instance subscription for intermediate topic", e);
                throw new RuntimeException(e);
            }
        }
    }

    private void cleanupBatchSource(Function.FunctionDetails functionDetails) {
        if (isBatchSource(functionDetails)) {
            // clean up intermediate topic
            String intermediateTopicName = SourceConfigUtils.computeBatchSourceIntermediateTopicName(functionDetails.getTenant(),
              functionDetails.getNamespace(), functionDetails.getName()).toString();
            String intermediateTopicSubscription = SourceConfigUtils.computeBatchSourceInstanceSubscriptionName(
              functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName());
            String fqfn = FunctionCommon.getFullyQualifiedName(functionDetails);
            try {
                Actions.newBuilder()
                  .addAction(
                    // Unsubscribe and allow time for consumers to close
                    Actions.Action.builder()
                      .actionName(String.format("Removing intermediate topic subscription %s for Batch Source %s",
                        intermediateTopicSubscription, fqfn))
                      .numRetries(10)
                      .sleepBetweenInvocationsMs(1000)
                      .supplier(
                        getDeleteSubscriptionSupplier(intermediateTopicName,
                          false,
                          intermediateTopicSubscription)
                      )
                      .build())
                  .addAction(
                    // Delete topic forcibly regardless whether unsubscribe succeeded or not
                    Actions.Action.builder()
                    .actionName(String.format("Deleting intermediate topic %s for Batch Source %s",
                      intermediateTopicName, fqfn))
                    .numRetries(10)
                    .sleepBetweenInvocationsMs(1000)
                    .supplier(getDeleteTopicSupplier(intermediateTopicName))
                    .build())
                  .run();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static boolean isBatchSource(Function.FunctionDetails functionDetails) {
        if (InstanceUtils.calculateSubjectType(functionDetails) == FunctionDetails.ComponentType.SOURCE) {
            String fqfn = FunctionCommon.getFullyQualifiedName(functionDetails);
            Map<String, Object> configMap = SourceConfigUtils.extractSourceConfig(functionDetails.getSource(), fqfn);
            if (configMap != null) {
                BatchSourceConfig batchSourceConfig = SourceConfigUtils.extractBatchSourceConfig(configMap);
                if (batchSourceConfig != null) {
                    return true;
                }
            }
        }
        return false;
    }
}
