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

import java.io.IOException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.functions.worker.rest.WorkerServer;
import org.apache.pulsar.functions.worker.service.WorkerServiceLoader;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class Worker {

    private final WorkerConfig workerConfig;
    private final WorkerService workerService;
    private WorkerServer server;

    private final OrderedExecutor orderedExecutor = OrderedExecutor.newBuilder().numThreads(8).name("zk-cache-ordered").build();
    private PulsarResources pulsarResources;
    private MetadataStoreExtended configMetadataStore;
    private final ErrorNotifier errorNotifier;

    public Worker(WorkerConfig workerConfig) {
        this.workerConfig = workerConfig;
        this.workerService = WorkerServiceLoader.load(workerConfig);
        this.errorNotifier = ErrorNotifier.getDefaultImpl();
    }

    protected void start() throws Exception {
        workerService.initAsStandalone(workerConfig);
        workerService.start(getAuthenticationService(), getAuthorizationService(), errorNotifier);
        server = new WorkerServer(workerService, getAuthenticationService());
        server.start();
        log.info("/** Started worker server on port={} **/", this.workerConfig.getWorkerPort());

        try {
            errorNotifier.waitForError();
        } catch (Throwable th) {
            log.error("!-- Fatal error encountered. Worker will exit now. --!", th);
            throw th;
        }
    }



    private AuthorizationService getAuthorizationService() throws PulsarServerException {

        if (this.workerConfig.isAuthorizationEnabled()) {

            log.info("starting configuration cache service");
            try {
                configMetadataStore = PulsarResources.createMetadataStore(
                        workerConfig.getConfigurationMetadataStoreUrl(),
                        (int) workerConfig.getMetadataStoreSessionTimeoutMillis(),
                        workerConfig.isZooKeeperAllowReadOnlyOperations());
            } catch (IOException e) {
                throw new PulsarServerException(e);
            }
            pulsarResources = new PulsarResources(null, configMetadataStore);
            return new AuthorizationService(getServiceConfiguration(), this.pulsarResources);
            }
        return null;
    }

    private AuthenticationService getAuthenticationService() throws PulsarServerException {
        return new AuthenticationService(getServiceConfiguration());
    }

    protected void stop() {
        try {
            if (null != this.server) {
                this.server.stop();
            }
            workerService.stop();
        } catch(Exception e) {
            log.warn("Failed to gracefully stop worker service ", e);
        }

        if (this.configMetadataStore != null) {
            try {
                this.configMetadataStore.close();
            } catch (Exception e) {
                log.warn("Failed to close global zk cache ", e);
            }
        }

        if (orderedExecutor != null) {
            orderedExecutor.shutdownNow();
        }
    }


    public Optional<Integer> getListenPortHTTP() {
        return this.server.getListenPortHTTP();
    }

    public Optional<Integer> getListenPortHTTPS() {
        return this.server.getListenPortHTTPS();
    }

    private ServiceConfiguration getServiceConfiguration() {
        ServiceConfiguration serviceConfiguration = PulsarConfigurationLoader.convertFrom(workerConfig);
        serviceConfiguration.setClusterName(workerConfig.getPulsarFunctionsCluster());
        return serviceConfiguration;
    }
}
