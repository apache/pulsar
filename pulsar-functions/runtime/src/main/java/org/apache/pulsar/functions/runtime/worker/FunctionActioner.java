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
package org.apache.pulsar.functions.runtime.worker;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.container.FunctionContainerFactory;
import org.apache.pulsar.functions.runtime.spawner.LimitsConfig;
import org.apache.pulsar.functions.runtime.spawner.Spawner;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Slf4j
public class FunctionActioner implements AutoCloseable {

    @Data
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    @Slf4j
    public static class AssignmentInfo {
        private FunctionMetaData functionMetaData;
        private Spawner spawner;
    }

    private final WorkerConfig workerConfig;
    private final LimitsConfig limitsConfig;
    private final FunctionContainerFactory functionContainerFactory;
    private final Namespace dlogNamespace;
    private LinkedBlockingQueue<FunctionAction> actionQueue;
    private volatile boolean running;
    private Thread actioner;
    private final Map<String, AssignmentInfo> assignments = new HashMap<>();

    public FunctionActioner(WorkerConfig workerConfig, LimitsConfig limitsConfig,
                            FunctionContainerFactory functionContainerFactory,
                            Namespace dlogNamespace,
                            LinkedBlockingQueue<FunctionAction> actionQueue) {
        this.workerConfig = workerConfig;
        this.limitsConfig = limitsConfig;
        this.functionContainerFactory = functionContainerFactory;
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
                            startFunction(action.getFunctionMetaData());
                        } catch (Exception ex) {
                            log.info("Error starting function", ex);
                            action.getFunctionMetaData().setStartupException(ex);
                        }
                    } else {
                        stopFunction(action.getFunctionMetaData());
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

    private void startFunction(FunctionMetaData functionMetaData) throws Exception {
        log.info("Starting function {} ...", functionMetaData.getFunctionConfig().getName());
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

        File pkgFile = new File(pkgDir, new File(functionMetaData.getPackageLocation().getPackagePath()).getName());
        if (!pkgFile.exists()) {
            log.info("Function package file {} doesn't exist, downloading from {}",
                    pkgFile, functionMetaData.getPackageLocation());
            Utils.downloadFromBookkeeper(
                    dlogNamespace,
                    new FileOutputStream(pkgFile),
                    functionMetaData.getPackageLocation().getPackagePath());
        }
        Spawner spawner = Spawner.createSpawner(functionMetaData.getFunctionConfig(), limitsConfig,
                pkgFile.getAbsolutePath(), functionContainerFactory);

        AssignmentInfo assignmentInfo = new AssignmentInfo();
        assignmentInfo.setFunctionMetaData(functionMetaData);
        assignmentInfo.setSpawner(spawner);
        assignments.put(functionMetaData.getFunctionConfig().getFullyQualifiedName(), assignmentInfo);
        spawner.start();
    }

    private boolean stopFunction(FunctionMetaData functionMetaData) {
        log.info("Stopping function {}...", functionMetaData.getFunctionConfig().getName());
        AssignmentInfo assignmentInfo = assignments.get(functionMetaData.getFunctionConfig().getFullyQualifiedName());
        if (assignmentInfo != null && assignmentInfo.getSpawner() != null) {
            assignmentInfo.getSpawner().close();
            assignmentInfo.setSpawner(null);
            return true;
        }
        return false;
    }

    public boolean containsAssignment(FunctionConfig functionConfig) {
        return assignments.containsKey(functionConfig.getFullyQualifiedName());
    }

    public Spawner getSpawner(FunctionConfig functionConfig) {
        if (!containsAssignment(functionConfig)) {
            return null;
        } else {
            return assignments.get(functionConfig.getFullyQualifiedName()).getSpawner();
        }
    }
}
