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
package org.apache.pulsar.functions.worker.service;

import java.io.IOException;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.functions.worker.ErrorNotifier;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.api.Functions;
import org.apache.pulsar.functions.worker.service.api.FunctionsV2;
import org.apache.pulsar.functions.worker.service.api.Sinks;
import org.apache.pulsar.functions.worker.service.api.Sources;
import org.apache.pulsar.functions.worker.service.api.Workers;
import org.apache.pulsar.zookeeper.ZooKeeperCache;

/**
 * A worker service with its classloader.
 */
@Slf4j
@Data
@RequiredArgsConstructor
public class WorkerServiceWithClassLoader implements WorkerService {

    private final WorkerService service;
    private final NarClassLoader classLoader;

    @Override
    public WorkerConfig getWorkerConfig() {
        return service.getWorkerConfig();
    }

    @Override
    public void initAsStandalone(WorkerConfig workerConfig) throws Exception {
        service.initAsStandalone(workerConfig);
    }

    @Override
    public void initInBroker(ServiceConfiguration brokerConfig,
                             WorkerConfig workerConfig,
                             PulsarResources pulsarResources,
                             InternalConfigurationData internalConf) throws Exception {
        service.initInBroker(brokerConfig, workerConfig, pulsarResources, internalConf);
    }

    @Override
    public void start(AuthenticationService authenticationService,
                      AuthorizationService authorizationService,
                      ErrorNotifier errorNotifier) throws Exception {
        service.start(authenticationService, authorizationService, errorNotifier);
    }

    @Override
    public void stop() {
        service.stop();
        try {
            classLoader.close();
        } catch (IOException e) {
            log.warn("Failed to close the worker service class loader", e);
        }
    }

    @Override
    public boolean isInitialized() {
        return service.isInitialized();
    }

    @Override
    public Functions<? extends WorkerService> getFunctions() {
        return service.getFunctions();
    }

    @Override
    public FunctionsV2<? extends WorkerService> getFunctionsV2() {
        return service.getFunctionsV2();
    }

    @Override
    public Sinks<? extends WorkerService> getSinks() {
        return service.getSinks();
    }

    @Override
    public Sources<? extends WorkerService> getSources() {
        return service.getSources();
    }

    @Override
    public Workers<? extends WorkerService> getWorkers() {
        return service.getWorkers();
    }

    @Override
    public void generateFunctionsStats(SimpleTextOutputStream out) {
        service.generateFunctionsStats(out);
    }

}
