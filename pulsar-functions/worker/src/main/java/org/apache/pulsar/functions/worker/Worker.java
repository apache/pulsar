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

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.functions.worker.rest.WorkerServer;
import org.apache.pulsar.zookeeper.GlobalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class Worker {

    private final WorkerConfig workerConfig;
    private final WorkerService workerService;
    private WorkerServer server;

    private ZooKeeperClientFactory zkClientFactory = null;
    private final OrderedExecutor orderedExecutor = OrderedExecutor.newBuilder().numThreads(8).name("zk-cache-ordered").build();
    private final ScheduledExecutorService cacheExecutor = Executors.newScheduledThreadPool(10,
            new DefaultThreadFactory("zk-cache-callback"));
    private GlobalZooKeeperCache globalZkCache;
    private ConfigurationCacheService configurationCacheService;

    public Worker(WorkerConfig workerConfig) {
        this.workerConfig = workerConfig;
        this.workerService = new WorkerService(workerConfig);
    }

    protected void start() throws Exception {
        URI dlogUri = initialize(this.workerConfig);

        workerService.start(dlogUri, getAuthenticationService(), getAuthorizationService());
        this.server = new WorkerServer(workerService);
        this.server.start();
        log.info("Start worker server on port {}...", this.workerConfig.getWorkerPort());
    }

    private static URI initialize(WorkerConfig workerConfig)
            throws InterruptedException, PulsarAdminException, IOException {
        // initializing pulsar functions namespace
        PulsarAdmin admin = WorkerUtils.getPulsarAdminClient(workerConfig.getPulsarWebServiceUrl(),
                workerConfig.getClientAuthenticationPlugin(), workerConfig.getClientAuthenticationParameters(),
                workerConfig.getTlsTrustCertsFilePath(), workerConfig.isTlsAllowInsecureConnection(),
                workerConfig.isTlsHostnameVerificationEnable());
        InternalConfigurationData internalConf;
        // make sure pulsar broker is up
        log.info("Checking if pulsar service at {} is up...", workerConfig.getPulsarWebServiceUrl());
        int maxRetries = workerConfig.getInitialBrokerReconnectMaxRetries();
        int retries = 0;
        while (true) {
            try {
                admin.clusters().getClusters();
                break;
            } catch (PulsarAdminException e) {
                log.warn("Failed to retrieve clusters from pulsar service", e);
                log.warn("Retry to connect to Pulsar service at {}", workerConfig.getPulsarWebServiceUrl());
                if (retries >= maxRetries) {
                    log.error("Failed to connect to Pulsar service at {} after {} attempts",
                            workerConfig.getPulsarFunctionsNamespace(), maxRetries);
                    throw e;
                }
                retries ++;
                Thread.sleep(1000);
            }
        }

        // getting namespace policy
        log.info("Initializing Pulsar Functions namespace...");
        try {
            try {
                admin.namespaces().getPolicies(workerConfig.getPulsarFunctionsNamespace());
            } catch (PulsarAdminException e) {
                if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                    // if not found than create
                    try {
                        Policies policies = new Policies();
                        policies.retention_policies = new RetentionPolicies(-1, -1);
                        policies.replication_clusters = new HashSet<>();
                        policies.replication_clusters.add(workerConfig.getPulsarFunctionsCluster());
                        admin.namespaces().createNamespace(workerConfig.getPulsarFunctionsNamespace(),
                                policies);
                    } catch (PulsarAdminException e1) {
                        // prevent race condition with other workers starting up
                        if (e1.getStatusCode() != Response.Status.CONFLICT.getStatusCode()) {
                            log.error("Failed to create namespace {} for pulsar functions", workerConfig
                                    .getPulsarFunctionsNamespace(), e1);
                            throw e1;
                        }
                    }
                } else {
                    log.error("Failed to get retention policy for pulsar function namespace {}",
                            workerConfig.getPulsarFunctionsNamespace(), e);
                    throw e;
                }
            }
            try {
                internalConf = admin.brokers().getInternalConfigurationData();
            } catch (PulsarAdminException e) {
                log.error("Failed to retrieve broker internal configuration", e);
                throw e;
            }
        } finally {
            admin.close();
        }

        // initialize the dlog namespace
        // TODO: move this as part of pulsar cluster initialization later
        try {
            return WorkerUtils.initializeDlogNamespace(
                    internalConf.getZookeeperServers(),
                    internalConf.getLedgersRootPath());
        } catch (IOException ioe) {
            log.error("Failed to initialize dlog namespace at zookeeper {} for storing function packages",
                    internalConf.getZookeeperServers(), ioe);
            throw ioe;
        }
    }

    private AuthorizationService getAuthorizationService() throws PulsarServerException {

        if (this.workerConfig.isAuthorizationEnabled()) {

            log.info("starting configuration cache service");

            this.globalZkCache = new GlobalZooKeeperCache(getZooKeeperClientFactory(),
                    (int) workerConfig.getZooKeeperSessionTimeoutMillis(),
                    workerConfig.getZooKeeperOperationTimeoutSeconds(),
                    workerConfig.getConfigurationStoreServers(),
                    orderedExecutor, cacheExecutor,
                    workerConfig.getZooKeeperOperationTimeoutSeconds());
            try {
                this.globalZkCache.start();
            } catch (IOException e) {
                throw new PulsarServerException(e);
            }

            this.configurationCacheService = new ConfigurationCacheService(this.globalZkCache, this.workerConfig.getPulsarFunctionsCluster());
                return new AuthorizationService(PulsarConfigurationLoader.convertFrom(workerConfig), this.configurationCacheService);
            }
        return null;
    }

    private AuthenticationService getAuthenticationService() throws PulsarServerException {
        return new AuthenticationService(PulsarConfigurationLoader.convertFrom(workerConfig));
    }

    public ZooKeeperClientFactory getZooKeeperClientFactory() {
        if (zkClientFactory == null) {
            zkClientFactory = new ZookeeperBkClientFactoryImpl(orderedExecutor);
        }
        // Return default factory
        return zkClientFactory;
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

        if (this.globalZkCache != null) {
            try {
                this.globalZkCache.close();
            } catch (IOException e) {
                log.warn("Failed to close global zk cache ", e);
            }
        }
    }


    public Optional<Integer> getListenPortHTTP() {
        return this.server.getListenPortHTTP();
    }

    public Optional<Integer> getListenPortHTTPS() {
        return this.server.getListenPortHTTPS();
    }
}
