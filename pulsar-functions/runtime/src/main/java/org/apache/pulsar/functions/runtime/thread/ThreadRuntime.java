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
package org.apache.pulsar.functions.runtime.thread;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.nar.FileUtils;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.instance.JavaInstanceRunnable;
import org.apache.pulsar.functions.instance.stats.FunctionCollectorRegistry;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.proto.FunctionStatus;
import org.apache.pulsar.functions.proto.MetricsData;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.FunctionsManager;

/**
 * A function container implemented using java thread.
 */
@CustomLog
public class ThreadRuntime implements Runtime {

    // The thread that invokes the function
    private Thread fnThread;

    private static final int THREAD_SHUTDOWN_TIMEOUT_MILLIS = 10_000;

    @Getter
    private final InstanceConfig instanceConfig;
    private JavaInstanceRunnable javaInstanceRunnable;
    private final ThreadGroup threadGroup;
    private final FunctionCacheManager fnCache;
    private final String jarFile;
    private final String transformFunctionFile;
    private final ClientBuilder clientBuilder;
    private final PulsarClient pulsarClient;
    private final PulsarAdmin pulsarAdmin;
    private final String stateStorageImplClass;
    private final String stateStorageServiceUrl;
    private final SecretsProvider secretsProvider;
    private final FunctionCollectorRegistry collectorRegistry;
    private final String narExtractionDirectory;
    private final Optional<ConnectorsManager> connectorsManager;
    private final Optional<FunctionsManager> functionsManager;

    ThreadRuntime(InstanceConfig instanceConfig,
                  FunctionCacheManager fnCache,
                  ThreadGroup threadGroup,
                  String jarFile,
                  String transformFunctionFile,
                  PulsarClient client,
                  ClientBuilder clientBuilder,
                  PulsarAdmin pulsarAdmin,
                  String stateStorageImplClass,
                  String stateStorageServiceUrl,
                  SecretsProvider secretsProvider,
                  FunctionCollectorRegistry collectorRegistry,
                  String narExtractionDirectory,
                  Optional<ConnectorsManager> connectorsManager,
                  Optional<FunctionsManager> functionsManager) {
        this.instanceConfig = instanceConfig;
        if (instanceConfig.getFunctionDetails().getRuntime() != FunctionDetails.Runtime.JAVA) {
            throw new RuntimeException("Thread Container only supports Java Runtime");
        }

        this.threadGroup = threadGroup;
        this.fnCache = fnCache;
        this.jarFile = jarFile;
        this.transformFunctionFile = transformFunctionFile;
        this.clientBuilder = clientBuilder;
        this.pulsarClient = client;
        this.pulsarAdmin = pulsarAdmin;
        this.stateStorageImplClass = stateStorageImplClass;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.secretsProvider = secretsProvider;
        this.collectorRegistry = collectorRegistry;
        this.narExtractionDirectory = narExtractionDirectory;
        this.connectorsManager = connectorsManager;
        this.functionsManager = functionsManager;
    }

    private static ClassLoader getFunctionClassLoader(InstanceConfig instanceConfig,
                                                      String functionId,
                                                      String jarFile,
                                                      String narExtractionDirectory,
                                                      FunctionCacheManager fnCache,
                                                      Optional<ConnectorsManager> connectorsManager,
                                                      Optional<FunctionsManager> functionsManager,
                                                      FunctionDetails.ComponentType componentType)
            throws Exception {
        if (FunctionCommon.isFunctionCodeBuiltin(instanceConfig.getFunctionDetails(), componentType)) {
            if (componentType == FunctionDetails.ComponentType.FUNCTION && functionsManager.isPresent()) {
                return functionsManager.get()
                        .getFunction(instanceConfig.getFunctionDetails().getBuiltin())
                        .getFunctionPackage().getClassLoader();
            }
            if (componentType == FunctionDetails.ComponentType.SOURCE && connectorsManager.isPresent()) {
                return connectorsManager.get()
                        .getConnector(instanceConfig.getFunctionDetails().getSource().getBuiltin())
                        .getConnectorFunctionPackage().getClassLoader();
            }
            if (componentType == FunctionDetails.ComponentType.SINK && connectorsManager.isPresent()) {
                return connectorsManager.get()
                        .getConnector(instanceConfig.getFunctionDetails().getSink().getBuiltin())
                        .getConnectorFunctionPackage().getClassLoader();
            }
        }
        return loadJars(jarFile, instanceConfig, functionId, instanceConfig.getFunctionDetails().getName(),
                narExtractionDirectory, fnCache);
    }

    public static ClassLoader loadJars(String jarFile,
                                       InstanceConfig instanceConfig,
                                       String functionId,
                                       String functionName,
                                       String narExtractionDirectory,
                                       FunctionCacheManager fnCache) throws Exception {
        if (jarFile == null) {
            return Thread.currentThread().getContextClassLoader();
        }
        ClassLoader fnClassLoader;
        boolean loadedAsNar = false;
        if (FileUtils.mayBeANarArchive(new File(jarFile))) {
            try {
                log.info().attr("jarFile", jarFile).log("Trying Loading file as NAR file");
                // Let's first try to treat it as a nar archive
                fnCache.registerFunctionInstanceWithArchive(
                        functionId,
                        instanceConfig.getInstanceName(),
                        jarFile, narExtractionDirectory);
                loadedAsNar = true;
            } catch (FileNotFoundException e) {
                // this is usually like
                // java.io.FileNotFoundException: /tmp/pulsar-nar/xxx.jar-unpacked/xxxxx/META-INF/MANIFEST.MF'
                log.error().attr("jarFile", jarFile).attr("error", e.toString())
                        .log("The file does not look like a .nar file");
            }
        }
        if (!loadedAsNar) {
            log.info().attr("jarFile", jarFile).log("Load file as simple JAR file");
            // create the function class loader
            fnCache.registerFunctionInstance(
                    functionId,
                    instanceConfig.getInstanceName(),
                    Arrays.asList(jarFile),
                    Collections.emptyList());
        }

        log.info().attr("functionName", functionName)
                .attr("functionClassLoader", fnCache.getClassLoader(functionId))
                .log("Initialize function class loader at function cache manager");

        fnClassLoader = fnCache.getClassLoader(functionId);
        if (null == fnClassLoader) {
            throw new Exception("No function class loader available.");
        }

        return fnClassLoader;
    }

    /**
     * The core logic that initialize the thread container and executes the function.
     */
    @Override
    public void start() throws Exception {

        // extract class loader for function
        ClassLoader functionClassLoader =
                getFunctionClassLoader(instanceConfig, instanceConfig.getFunctionId(), jarFile, narExtractionDirectory,
                        fnCache, connectorsManager, functionsManager,
                        InstanceUtils.calculateSubjectType(instanceConfig.getFunctionDetails()));

        ClassLoader transformFunctionClassLoader = transformFunctionFile == null ? null : getFunctionClassLoader(
                instanceConfig, instanceConfig.getTransformFunctionId(), transformFunctionFile, narExtractionDirectory,
                fnCache, connectorsManager, functionsManager, FunctionDetails.ComponentType.FUNCTION);

        // re-initialize JavaInstanceRunnable so that variables in constructor can be re-initialized
        this.javaInstanceRunnable = new JavaInstanceRunnable(
                instanceConfig,
                clientBuilder,
                pulsarClient,
                pulsarAdmin,
                stateStorageImplClass,
                stateStorageServiceUrl,
                secretsProvider,
                collectorRegistry,
                functionClassLoader,
                transformFunctionClassLoader);

        log.info().attr("instanceId", instanceConfig.getInstanceId())
                .attr("functionId", instanceConfig.getFunctionId())
                .attr("namespace", instanceConfig.getFunctionDetails().getNamespace())
                .log("ThreadContainer starting function");
        this.fnThread = new Thread(threadGroup, javaInstanceRunnable,
                String.format("%s-%s",
                        FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()),
                        instanceConfig.getInstanceId()));
        this.fnThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.error().attr("thread", t).exception(e)
                        .log("Uncaught exception in thread");
            }
        });
        this.fnThread.start();
    }

    @Override
    public void join() throws Exception {
        if (this.fnThread != null) {
            this.fnThread.join();
        }
    }

    @Override
    public void stop() {
        if (fnThread != null) {
            // interrupt the instance thread
            fnThread.interrupt();
            try {
                // If the instance thread doesn't respond within some time, attempt to
                // kill the thread
                fnThread.join(THREAD_SHUTDOWN_TIMEOUT_MILLIS, 0);
                if (fnThread.isAlive()) {
                    log.warn().attr("timeoutMs", THREAD_SHUTDOWN_TIMEOUT_MILLIS)
                            .log("The function instance thread is still alive after timeout."
                                    + " Giving up waiting and moving forward to close function");
                }
            } catch (InterruptedException e) {
                // ignore this
            }
            // make sure JavaInstanceRunnable is closed
            this.javaInstanceRunnable.close();

            log.info().attr("instanceId", instanceConfig.getInstanceId())
                    .attr("functionId", instanceConfig.getFunctionId())
                    .attr("namespace", instanceConfig.getFunctionDetails().getNamespace())
                    .log("Unloading JAR files");
            // once the thread quits, clean up the instance
            fnCache.unregisterFunctionInstance(
                    instanceConfig.getFunctionId(),
                    instanceConfig.getInstanceName());
        }
    }

    @Override
    public CompletableFuture<FunctionStatus> getFunctionStatus(int instanceId) {
        CompletableFuture<FunctionStatus> statsFuture = new CompletableFuture<>();
        if (!isAlive()) {
            FunctionStatus functionStatus = new FunctionStatus();
            functionStatus.setRunning(false);
            Throwable ex = getDeathException();
            if (ex != null && ex.getMessage() != null) {
                functionStatus.setFailureException(ex.getMessage());
            }
            statsFuture.complete(functionStatus);
            return statsFuture;
        }
        FunctionStatus functionStatus = javaInstanceRunnable.getFunctionStatus();
        functionStatus.setRunning(true);
        statsFuture.complete(functionStatus);
        return statsFuture;
    }

    @Override
    public CompletableFuture<MetricsData> getAndResetMetrics() {
        return CompletableFuture.completedFuture(javaInstanceRunnable.getAndResetMetrics());
    }


    @Override
    public CompletableFuture<MetricsData> getMetrics(int instanceId) {
        return CompletableFuture.completedFuture(javaInstanceRunnable.getMetrics());
    }

    @Override
    public String getPrometheusMetrics() throws IOException {
        if (javaInstanceRunnable == null) {
            throw new PulsarServerException("javaInstanceRunnable is not initialized");
        }
        return javaInstanceRunnable.getStatsAsString();
    }

    @Override
    public CompletableFuture<Void> resetMetrics() {
        javaInstanceRunnable.resetMetrics();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isAlive() {
        if (this.fnThread != null) {
            return this.fnThread.isAlive();
        } else {
            return false;
        }
    }

    @Override
    public Throwable getDeathException() {
        if (isAlive()) {
            return null;
        } else if (null != javaInstanceRunnable) {
            return javaInstanceRunnable.getDeathException();
        } else {
            return null;
        }
    }
}
