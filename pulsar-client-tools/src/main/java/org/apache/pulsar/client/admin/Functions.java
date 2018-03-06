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

import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatusList;

import java.util.List;

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

}
