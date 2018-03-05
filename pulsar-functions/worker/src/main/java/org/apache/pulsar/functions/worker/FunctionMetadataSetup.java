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
import java.net.URI;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;

/**
 * Tools to setup function metadata.
 */
@Slf4j
public class FunctionMetadataSetup {

    /**
     * Setup function metadata.
     *
     * @param workerConfig worker config
     * @return dlog uri for store function jars
     * @throws InterruptedException interrupted at setting up metadata
     * @throws PulsarAdminException when encountering exception on pulsar admin operation
     * @throws IOException when create dlog namespace for storing function jars.
     */
    public static URI setupFunctionMetadata(WorkerConfig workerConfig)
            throws InterruptedException, PulsarAdminException, IOException {
        // initializing pulsar functions namespace
        PulsarAdmin admin = Utils.getPulsarAdminClient(workerConfig.getPulsarWebServiceUrl());
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
                        admin.namespaces().createNamespace(workerConfig.getPulsarFunctionsNamespace());
                    } catch (PulsarAdminException e1) {
                        // prevent race condition with other workers starting up
                        if (e1.getStatusCode() != Response.Status.CONFLICT.getStatusCode()) {
                            log.error("Failed to create namespace {} for pulsar functions", workerConfig
                                .getPulsarFunctionsNamespace(), e1);
                            throw e1;
                        }
                    }
                    try {
                        admin.namespaces().setRetention(
                            workerConfig.getPulsarFunctionsNamespace(),
                            new RetentionPolicies(Integer.MAX_VALUE, Integer.MAX_VALUE));
                    } catch (PulsarAdminException e1) {
                        log.error("Failed to set retention policy for pulsar functions namespace", e);
                        throw new RuntimeException(e1);
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
            return Utils.initializeDlogNamespace(
                internalConf.getZookeeperServers(),
                internalConf.getLedgersRootPath());
        } catch (IOException ioe) {
            log.error("Failed to initialize dlog namespace at zookeeper {} for storing function packages",
                    internalConf.getZookeeperServers(), ioe);
            throw ioe;
        }
    }

}
