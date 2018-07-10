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

import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatusList;

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
    FunctionDetails getFunction(String tenant, String namespace, String function) throws PulsarAdminException;

    /**
     * Create a new function.
     *
     * @param functionDetails
     *            the function configuration object
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createFunction(FunctionDetails functionDetails, String fileName) throws PulsarAdminException;

    /**
     * <pre>
     * Create a new function by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     * </pre>
     *
     * @param functionDetails
     *            the function configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws PulsarAdminException
     */
    void createFunctionWithUrl(FunctionDetails functionDetails, String pkgUrl) throws PulsarAdminException;

    /**
     * Update the configuration for a function.
     * <p>
     *
     * @param functionDetails
     *            the function configuration object
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateFunction(FunctionDetails functionDetails, String fileName) throws PulsarAdminException;

    /**
     * Update the configuration for a function.
     * <pre>
     * Update a function by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     * </pre>
     *
     * @param functionDetails
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
    void updateFunctionWithUrl(FunctionDetails functionDetails, String pkgUrl) throws PulsarAdminException;

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
    FunctionStatusList getFunctionStatus(String tenant, String namespace, String function) throws PulsarAdminException;

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
     * Fetches a list of supported Pulsar IO connectors currently running in cluster mode
     *
     * @throws PulsarAdminException
     *             Unexpected error
     *
     */
    List<ConnectorDefinition> getConnectorsList() throws PulsarAdminException;
}
