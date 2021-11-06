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

package org.apache.pulsar.functions.runtime.thread;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.nar.FileUtils;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.instance.stats.FunctionCollectorRegistry;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.instance.JavaInstanceRunnable;
import org.apache.pulsar.functions.worker.ConnectorsManager;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class ThreadRuntime implements Runtime {

    // The thread that invokes the function
    private Thread fnThread;

    private static final int THREAD_SHUTDOWN_TIMEOUT_MILLIS = 10_000;

    @Getter
    private InstanceConfig instanceConfig;
    private JavaInstanceRunnable javaInstanceRunnable;
    private ThreadGroup threadGroup;
    private FunctionCacheManager fnCache;
    private String jarFile;
    private ClientBuilder clientBuilder;
    private PulsarClient pulsarClient;
    private PulsarAdmin pulsarAdmin;
    private String stateStorageImplClass;
    private String stateStorageServiceUrl;
    private SecretsProvider secretsProvider;
    private FunctionCollectorRegistry collectorRegistry;
    private String narExtractionDirectory;
    private final Optional<ConnectorsManager> connectorsManager;

    ThreadRuntime(InstanceConfig instanceConfig,
                  FunctionCacheManager fnCache,
                  ThreadGroup threadGroup,
                  String jarFile,
                  PulsarClient client,
                  ClientBuilder clientBuilder,
                  PulsarAdmin pulsarAdmin,
                  String stateStorageImplClass,
                  String stateStorageServiceUrl,
                  SecretsProvider secretsProvider,
                  FunctionCollectorRegistry collectorRegistry,
                  String narExtractionDirectory,
                  Optional<ConnectorsManager> connectorsManager) {
        this.instanceConfig = instanceConfig;
        if (instanceConfig.getFunctionDetails().getRuntime() != Function.FunctionDetails.Runtime.JAVA) {
            throw new RuntimeException("Thread Container only supports Java Runtime");
        }

        this.threadGroup = threadGroup;
        this.fnCache = fnCache;
        this.jarFile = jarFile;
        this.clientBuilder = clientBuilder;
        this.pulsarClient = client;
        this.pulsarAdmin = pulsarAdmin;
        this.stateStorageImplClass = stateStorageImplClass;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.secretsProvider = secretsProvider;
        this.collectorRegistry = collectorRegistry;
        this.narExtractionDirectory = narExtractionDirectory;
        this.connectorsManager = connectorsManager;
    }

    private static ClassLoader getFunctionClassLoader(InstanceConfig instanceConfig,
                                                      String jarFile,
                                                      String narExtractionDirectory,
                                                      FunctionCacheManager fnCache,
                                                      Optional<ConnectorsManager> connectorsManager) throws Exception {
        if (FunctionCommon.isFunctionCodeBuiltin(instanceConfig.getFunctionDetails())
                && connectorsManager.isPresent()) {
            switch (InstanceUtils.calculateSubjectType(instanceConfig.getFunctionDetails())) {
                case SOURCE:
                    return connectorsManager.get().getConnector(
                            instanceConfig.getFunctionDetails().getSource().getBuiltin()).getClassLoader();
                case SINK:
                    return connectorsManager.get().getConnector(
                            instanceConfig.getFunctionDetails().getSink().getBuiltin()).getClassLoader();
                default:
                    return loadJars(jarFile, instanceConfig, narExtractionDirectory, fnCache);
            }
        } else {
            return loadJars(jarFile, instanceConfig, narExtractionDirectory, fnCache);
        }
    }

    private static ClassLoader loadJars(String jarFile,
                                 InstanceConfig instanceConfig,
                                 String narExtractionDirectory,
                                 FunctionCacheManager fnCache) throws Exception {
        if (jarFile == null) {
            return Thread.currentThread().getContextClassLoader();
        }
        ClassLoader fnClassLoader;
        boolean loadedAsNar = false;
        if (FileUtils.mayBeANarArchive(new File(jarFile))) {
            try {
                log.info("Trying Loading file as NAR file: {}", jarFile);
                // Let's first try to treat it as a nar archive
                fnCache.registerFunctionInstanceWithArchive(
                        instanceConfig.getFunctionId(),
                        instanceConfig.getInstanceName(),
                        jarFile, narExtractionDirectory);
                loadedAsNar = true;
            } catch (FileNotFoundException e) {
                // this is usually like
                // java.io.FileNotFoundException: /tmp/pulsar-nar/xxx.jar-unpacked/xxxxx/META-INF/MANIFEST.MF'
                log.error("The file {} does not look like a .nar file", jarFile, e.toString());
            }
        }
        if (!loadedAsNar) {
            log.info("Load file as simple JAR file: {}", jarFile);
            // create the function class loader
            fnCache.registerFunctionInstance(
                    instanceConfig.getFunctionId(),
                    instanceConfig.getInstanceName(),
                    Arrays.asList(jarFile),
                    Collections.emptyList());
        }

        log.info("Initialize function class loader for function {} at function cache manager, functionClassLoader: {}",
                instanceConfig.getFunctionDetails().getName(), fnCache.getClassLoader(instanceConfig.getFunctionId()));

        fnClassLoader = fnCache.getClassLoader(instanceConfig.getFunctionId());
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
        ClassLoader functionClassLoader = getFunctionClassLoader(instanceConfig, jarFile, narExtractionDirectory, fnCache, connectorsManager);

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
                functionClassLoader);

        log.info("ThreadContainer starting function with instance config {}", instanceConfig);
        this.fnThread = new Thread(threadGroup, javaInstanceRunnable,
                String.format("%s-%s",
                        FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()),
                        instanceConfig.getInstanceId()));
        this.fnThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.error("Uncaught exception in thread {}", t, e);
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

    @SuppressWarnings("deprecation")
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
                    log.warn("The function instance thread is still alive after {} milliseconds. Giving up waiting and moving forward to close function.", THREAD_SHUTDOWN_TIMEOUT_MILLIS);
                }
            } catch (InterruptedException e) {
                // ignore this
            }
            // make sure JavaInstanceRunnable is closed
            this.javaInstanceRunnable.close();

            log.info("Unloading JAR files for function {}", instanceConfig);
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
            FunctionStatus.Builder functionStatusBuilder = FunctionStatus.newBuilder();
            functionStatusBuilder.setRunning(false);
            Throwable ex = getDeathException();
            if (ex != null && ex.getMessage() != null) {
                functionStatusBuilder.setFailureException(ex.getMessage());
            }
            statsFuture.complete(functionStatusBuilder.build());
            return statsFuture;
        }
        FunctionStatus.Builder functionStatusBuilder = javaInstanceRunnable.getFunctionStatus();
        functionStatusBuilder.setRunning(true);
        statsFuture.complete(functionStatusBuilder.build());
        return statsFuture;
    }

    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getAndResetMetrics() {
        return CompletableFuture.completedFuture(javaInstanceRunnable.getAndResetMetrics());
    }


    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getMetrics(int instanceId) {
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
