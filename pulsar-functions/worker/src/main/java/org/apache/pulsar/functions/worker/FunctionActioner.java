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

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.metrics.MetricsSink;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Slf4j
public class FunctionActioner implements AutoCloseable {

    private final WorkerConfig workerConfig;
    private final RuntimeFactory runtimeFactory;
    private final MetricsSink metricsSink;
    private final int metricsCollectionInterval;
    private final Namespace dlogNamespace;
    private LinkedBlockingQueue<FunctionAction> actionQueue;
    private volatile boolean running;
    private Thread actioner;

    public FunctionActioner(WorkerConfig workerConfig,
                            RuntimeFactory runtimeFactory,
                            MetricsSink metricsSink,
                            int metricCollectionInterval,
                            Namespace dlogNamespace,
                            LinkedBlockingQueue<FunctionAction> actionQueue) {
        this.workerConfig = workerConfig;
        this.runtimeFactory = runtimeFactory;
        this.metricsSink = metricsSink;
        this.metricsCollectionInterval = metricCollectionInterval;
        this.dlogNamespace = dlogNamespace;
        this.actionQueue = actionQueue;
        actioner = new Thread(() -> {
            log.info("Starting Actioner Thread...");
            while(running) {
                try {
                    FunctionAction action = actionQueue.poll(1, TimeUnit.SECONDS);
                    if (action == null) continue;
                    if (action.getAction() == FunctionAction.Action.START) {
                        try {
                            startFunction(action.getFunctionRuntimeInfo());
                        } catch (Exception ex) {
                            log.info("Error starting function", ex);
                            action.getFunctionRuntimeInfo().setStartupException(ex);
                        }
                    } else {
                        stopFunction(action.getFunctionRuntimeInfo());
                    }
                } catch (InterruptedException ex) {
                }
            }
        });
        actioner.setName("FunctionActionerThread");
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

    private void startFunction(FunctionRuntimeInfo functionRuntimeInfo) throws Exception {
        Function.Instance instance = functionRuntimeInfo.getFunctionInstance();
        FunctionMetaData functionMetaData = instance.getFunctionMetaData();
        log.info("Starting function {} - {} ...",
                functionMetaData.getFunctionConfig().getName(), instance.getInstanceId());
        File pkgDir = new File(
                workerConfig.getDownloadDirectory(),
                StringUtils.join(
                        new String[]{
                                functionMetaData.getFunctionConfig().getTenant(),
                                functionMetaData.getFunctionConfig().getNamespace(),
                                functionMetaData.getFunctionConfig().getName(),
                        },
                        File.separatorChar));
        pkgDir.mkdirs();

        int instanceId = functionRuntimeInfo.getFunctionInstance().getInstanceId();

        File pkgFile = new File(
            pkgDir,
            new File(FunctionConfigUtils.getDownloadFileName(functionMetaData.getFunctionConfig())).getName());

        if (!pkgFile.exists()) {
            // download only when the package file doesn't exist
            File tempPkgFile;
            while (true) {
                tempPkgFile = new File(
                    pkgDir,
                    pkgFile.getName() + "." + instanceId + "." + UUID.randomUUID().toString());
                if (!tempPkgFile.exists() && tempPkgFile.createNewFile()) {
                    break;
                }
            }
            try {
                log.info("Function package file {} will be downloaded from {}",
                    tempPkgFile, functionMetaData.getPackageLocation());
                Utils.downloadFromBookkeeper(
                    dlogNamespace,
                    new FileOutputStream(tempPkgFile),
                    functionMetaData.getPackageLocation().getPackagePath());

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

        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionConfig(functionMetaData.getFunctionConfig());
        // TODO: set correct function id and version when features implemented
        instanceConfig.setFunctionId(UUID.randomUUID().toString());
        instanceConfig.setFunctionVersion(UUID.randomUUID().toString());
        instanceConfig.setInstanceId(String.valueOf(instanceId));
        instanceConfig.setMaxBufferedTuples(1024);
        RuntimeSpawner runtimeSpawner = new RuntimeSpawner(instanceConfig, pkgFile.getAbsolutePath(), runtimeFactory,
                metricsSink, metricsCollectionInterval);

        functionRuntimeInfo.setRuntimeSpawner(runtimeSpawner);
        runtimeSpawner.start();
    }

    private boolean stopFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        Function.Instance instance = functionRuntimeInfo.getFunctionInstance();
        FunctionMetaData functionMetaData = instance.getFunctionMetaData();
        log.info("Stopping function {} - {}...",
                functionMetaData.getFunctionConfig().getName(), instance.getInstanceId());
        if (functionRuntimeInfo.getRuntimeSpawner() != null) {
            functionRuntimeInfo.getRuntimeSpawner().close();
            functionRuntimeInfo.setRuntimeSpawner(null);
            return true;
        }
        return false;
    }
}
