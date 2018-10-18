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
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatusList;
import org.apache.pulsar.functions.utils.SinkConfig;

import java.util.List;
import java.util.Set;

/**
 * Admin interface for Sink management.
 */
public interface Sink {
    /**
     * Get the list of sinks.
     * <p>
     * Get the list of all the Pulsar Sinks.
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
    List<String> getSinks(String tenant, String namespace) throws PulsarAdminException;

    /**
     * Get the configuration for the specified sink.
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
     * @param sink
     *            Sink name
     *
     * @return the sink configuration
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to get the configuration of the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Function.FunctionDetails getSink(String tenant, String namespace, String sink) throws PulsarAdminException;

    /**
     * Create a new sink.
     *
     * @param sinkConfig
     *            the sink configuration object
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createSink(SinkConfig sinkConfig, String fileName) throws PulsarAdminException;

    /**
     * <pre>
     * Create a new sink by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     * </pre>
     *
     * @param sinkConfig
     *            the sink configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws PulsarAdminException
     */
    void createSinkWithUrl(SinkConfig sinkConfig, String pkgUrl) throws PulsarAdminException;

    /**
     * Update the configuration for a sink.
     * <p>
     *
     * @param sinkConfig
     *            the sink configuration object
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateSink(SinkConfig sinkConfig, String fileName) throws PulsarAdminException;

    /**
     * Update the configuration for a sink.
     * <pre>
     * Update a sink by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     * </pre>
     *
     * @param sinkConfig
     *            the sink configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateSinkWithUrl(SinkConfig sinkConfig, String pkgUrl) throws PulsarAdminException;

    /**
     * Delete an existing sink
     * <p>
     * Delete a sink
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
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
    void deleteSink(String tenant, String namespace, String sink) throws PulsarAdminException;

    /**
     * Gets the current status of a sink.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    FunctionStatusList getSinkStatus(String tenant, String namespace, String sink) throws PulsarAdminException;

    /**
     * Gets the current status of a sink instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     * @param id
     *            Sink instance-id
     * @return
     * @throws PulsarAdminException
     */
    FunctionStatus getSinkStatus(String tenant, String namespace, String sink, int id)
            throws PulsarAdminException;

    /**
     * Restart sink instance
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @param instanceId
     *            Sink instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void restartSink(String tenant, String namespace, String sink, int instanceId) throws PulsarAdminException;

    /**
     * Restart all sink instances
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void restartSink(String tenant, String namespace, String sink) throws PulsarAdminException;


    /**
     * Stop sink instance
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @param instanceId
     *            Sink instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void stopSink(String tenant, String namespace, String sink, int instanceId) throws PulsarAdminException;

    /**
     * Stop all sink instances
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void stopSink(String tenant, String namespace, String sink) throws PulsarAdminException;

    /**
     * Fetches a list of supported Pulsar IO sinks currently running in cluster mode
     *
     * @throws PulsarAdminException
     *             Unexpected error
     *
     */
    List<ConnectorDefinition> getBuiltInSinks() throws PulsarAdminException;
}
