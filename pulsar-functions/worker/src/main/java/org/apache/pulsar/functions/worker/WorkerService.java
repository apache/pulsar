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

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.functions.worker.service.api.Functions;
import org.apache.pulsar.functions.worker.service.api.FunctionsV2;
import org.apache.pulsar.functions.worker.service.api.Sinks;
import org.apache.pulsar.functions.worker.service.api.Sources;
import org.apache.pulsar.functions.worker.service.api.Workers;

/**
 * API service provides the ability to manage functions.
 */
public interface WorkerService {

    /**
     * Return the worker config.
     *
     * @return worker config
     */
    WorkerConfig getWorkerConfig();

    /**
     * Initialize the worker API service using the provided config.
     *
     * @param workerConfig the worker config
     * @throws Exception when fail to initialize the worker API service.
     */
    void initAsStandalone(WorkerConfig workerConfig)
        throws Exception;

    /**
     * Initialize the worker service in broker.
     *
     * @param brokerConfig broker config
     * @param workerConfig worker config
     * @param pulsarResources configuration metadata-store
     * @param internalConf pulsar internal configuration data
     * @throws Exception when failed to initialize the worker service in broker.
     */
    void initInBroker(ServiceConfiguration brokerConfig,
                      WorkerConfig workerConfig,
                      PulsarResources pulsarResources,
                      InternalConfigurationData internalConf) throws Exception;

    /**
     * Start the worker API service.
     *
     * @param authenticationService the authentication service.
     * @param authorizationService the authorization service.
     * @param errorNotifier error notifier.
     * @throws Exception when fail to start the worker API service.
     */
    void start(AuthenticationService authenticationService,
               AuthorizationService authorizationService,
               ErrorNotifier errorNotifier) throws Exception;

    /**
     * Stop the worker API service.
     */
    void stop();

    /**
     * Check if the worker service is initialized or not.
     *
     * @return true if the worker service is initialized otherwise false.
     */
    boolean isInitialized();

    /**
     * Get the functions service.
     *
     * @return the functions service.
     */
    Functions<? extends WorkerService> getFunctions();

    /**
     * Get the functions service (v2).
     *
     * <p>This is a legacy API service for supporting v2.
     *
     * @return the functions service (v2).
     */
    FunctionsV2<? extends WorkerService> getFunctionsV2();

    /**
     * Get the sinks service.
     *
     * @return the sinks service.
     */
    Sinks<? extends WorkerService> getSinks();

    /**
     * Get the sources service.
     *
     * @return the sources service.
     */
    Sources<? extends WorkerService> getSources();

    /**
     * Get the worker service.
     *
     * @return the worker service.
     */
    Workers<? extends WorkerService> getWorkers();

    /**
     * Generate functions stats.
     *
     * @param out output stream
     */
    void generateFunctionsStats(SimpleTextOutputStream out);

}
