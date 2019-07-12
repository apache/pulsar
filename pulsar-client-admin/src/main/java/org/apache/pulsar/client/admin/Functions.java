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
package org.apache.pulsar.client.admin;

import java.util.List;
import java.util.Set;

import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;

/**
 * Admin interface for function management.
 */
public interface Functions {
    /**
     * Get the list of functions.
     * <p>
     * Get the list of all the Pulsar functions.
     * <p>
     * Response Example:
     *
     * <pre>
     * <code>["f1", "f2", "f3"]</code>
     * </pre>
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getFunctions(String tenant, String namespace) throws PulsarAdminException;

    /**
     * Get the configuration for the specified function.
     * <p>
     * Response Example:
     *
     * <pre>
     * <code>{ serviceUrl : "http://my-broker.example.com:8080/" }</code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     *
     * @return the function configuration
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to get the configuration of the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    FunctionConfig getFunction(String tenant, String namespace, String function) throws PulsarAdminException;

    /**
     * Create a new function.
     *
     * @param functionConfig
     *            the function configuration object
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException;

    /**
     * <pre>
     * Create a new function by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     * </pre>
     *
     * @param functionConfig
     *            the function configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws PulsarAdminException
     */
    void createFunctionWithUrl(FunctionConfig functionConfig, String pkgUrl) throws PulsarAdminException;

    /**
     * Update the configuration for a function.
     * <p>
     *
     * @param functionConfig
     *            the function configuration object
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException;

    /**
     * Update the configuration for a function.
     * <p>
     *
     * @param functionConfig
     *            the function configuration object
     * @param updateOptions
     *            options for the update operations
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateFunction(FunctionConfig functionConfig, String fileName, UpdateOptions updateOptions) throws PulsarAdminException;

    /**
     * Update the configuration for a function.
     * <pre>
     * Update a function by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     * </pre>
     *
     * @param functionConfig
     *            the function configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateFunctionWithUrl(FunctionConfig functionConfig, String pkgUrl) throws PulsarAdminException;

    /**
     * Update the configuration for a function.
     * <pre>
     * Update a function by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     * </pre>
     *
     * @param functionConfig
     *            the function configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @param updateOptions
     *            options for the update operations
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateFunctionWithUrl(FunctionConfig functionConfig, String pkgUrl, UpdateOptions updateOptions) throws PulsarAdminException;


    /**
     * Delete an existing function
     * <p>
     * Delete a function
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Cluster does not exist
     * @throws PreconditionFailedException
     *             Cluster is not empty
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteFunction(String tenant, String namespace, String function) throws PulsarAdminException;

    /**
     * Gets the current status of a function.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    FunctionStatus getFunctionStatus(String tenant, String namespace, String function) throws PulsarAdminException;

    /**
     * Gets the current status of a function instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     * @param id
     *            Function instance-id
     * @return
     * @throws PulsarAdminException
     */
    FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionStatus(String tenant, String namespace, String function, int id)
            throws PulsarAdminException;

    /**
     * Gets the current stats of a function instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     * @param id
     *            Function instance-id
     * @return
     * @throws PulsarAdminException
     */
    FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData getFunctionStats(String tenant, String namespace, String function, int id)
            throws PulsarAdminException;

    /**
     * Gets the current stats of a function.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     * @return
     * @throws PulsarAdminException
     */

    FunctionStats getFunctionStats(String tenant, String namespace, String function)
            throws PulsarAdminException;

    /**
     * Restart function instance
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     *
     * @param instanceId
     *            Function instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void restartFunction(String tenant, String namespace, String function, int instanceId) throws PulsarAdminException;

    /**
     * Restart all function instances
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void restartFunction(String tenant, String namespace, String function) throws PulsarAdminException;


    /**
     * Stop function instance
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     *
     * @param instanceId
     *            Function instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void stopFunction(String tenant, String namespace, String function, int instanceId) throws PulsarAdminException;

    /**
     * Start all function instances
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void startFunction(String tenant, String namespace, String function) throws PulsarAdminException;

    /**
     * Start function instance
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     *
     * @param instanceId
     *            Function instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void startFunction(String tenant, String namespace, String function, int instanceId) throws PulsarAdminException;

    /**
     * Stop all function instances
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void stopFunction(String tenant, String namespace, String function) throws PulsarAdminException;


    /**
     * Triggers the function by writing to the input topic.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     * @param triggerValue
     *            The input that will be written to input topic
     * @param triggerFile
     *            The file which contains the input that will be written to input topic
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    String triggerFunction(String tenant, String namespace, String function, String topic, String triggerValue, String triggerFile) throws PulsarAdminException;

    /**
     * Upload Data.
     *
     * @param sourceFile
     *            dataFile that needs to be uploaded
     * @param path
     *            Path where data should be stored
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void uploadFunction(String sourceFile, String path) throws PulsarAdminException;

    /**
     * Download Function Code.
     *
     * @param destinationFile
     *            file where data should be downloaded to
     * @param path
     *            Path where data is located
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void downloadFunction(String destinationFile, String path) throws PulsarAdminException;

    /**
     * Download Function Code.
     *
     * @param destinationFile
     *           file where data should be downloaded to
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     * @throws PulsarAdminException
     */
    void downloadFunction(String destinationFile, String tenant, String namespace, String function) throws PulsarAdminException;

    /**
     * Deprecated in favor of getting sources and sinks for their own APIs
     *
     * Fetches a list of supported Pulsar IO connectors currently running in cluster mode
     *
     * @throws PulsarAdminException
     *             Unexpected error
     *
     */
    @Deprecated
    List<ConnectorDefinition> getConnectorsList() throws PulsarAdminException;

    /**
     * Deprecated in favor of getting sources and sinks for their own APIs
     *
     * Fetches a list of supported Pulsar IO sources currently running in cluster mode
     *
     * @throws PulsarAdminException
     *             Unexpected error
     *
     */
    @Deprecated
    Set<String> getSources() throws PulsarAdminException;

    /**
     * Deprecated in favor of getting sources and sinks for their own APIs
     *
     * Fetches a list of supported Pulsar IO sinks currently running in cluster mode
     *
     * @throws PulsarAdminException
     *             Unexpected error
     *
     */
    @Deprecated
    Set<String> getSinks() throws PulsarAdminException;

    /**
     * Fetch the current state associated with a Pulsar Function.
     * <p>
     * Response Example:
     *
     * <pre>
     * <code>{ "value : 12, version : 2"}</code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     * @param key
     *            Key name of State
     *
     * @return the function configuration
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to get the configuration of the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    FunctionState getFunctionState(String tenant, String namespace, String function, String key) throws PulsarAdminException;

    /**
     * Puts the given state associated with a Pulsar Function.
     * <p>
     * Response Example:
     *
     * <pre>
     * <code>{ "value : 12, version : 2"}</code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param function
     *            Function name
     * @param state
     *            FunctionState
     **
     * @throws NotAuthorizedException
     *             You don't have admin permission to get the configuration of the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void putFunctionState(String tenant, String namespace, String function, FunctionState state) throws PulsarAdminException;
}
