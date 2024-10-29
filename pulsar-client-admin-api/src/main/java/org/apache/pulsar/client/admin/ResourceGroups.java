/*
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
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.policies.data.ResourceGroup;

/**
 * Admin interface for ResourceGroups management.
 */

public interface ResourceGroups {

        /**
         * Get the list of resourcegroups.
         * <p/>
         * Get the list of all the resourcegroup.
         * <p/>
         * Response Example:
         *
         * <pre>
         * <code>["resourcegroup1",
         *  "resourcegroup2",
         *  "resourcegroup3"]</code>
         * </pre>
         *
         * @throws PulsarAdminException.NotAuthorizedException Don't have admin permission
         * @throws PulsarAdminException                        Unexpected error
         */
        List<String> getResourceGroups() throws PulsarAdminException;

        /**
         * Get the list of resourcegroups asynchronously.
         * <p/>
         * Get the list of all the resourcegrops.
         * <p/>
         * Response Example:
         *
         * <pre>
         * <code>["resourcegroup1",
         *  "resourcegroup2",
         *  "resourcegroup3"]</code>
         * </pre>
         */
        CompletableFuture<List<String>> getResourceGroupsAsync();


        /**
         * Get configuration for a resourcegroup.
         * <p/>
         * Get configuration specified for a resourcegroup.
         * <p/>
         * Response Example:
         *
         * <pre>
         * <code>
         *     "publishRateInMsgs" : "value",
         *     "PublishRateInBytes" : "value",
         *     "DispatchRateInMsgs" : "value",
         *     "DispatchRateInBytes" : "value"
         *     "ReplicationDispatchRateInMsgs" : "value"
         *     "ReplicationDispatchRateInBytes" : "value"
         * </code>
         * </pre>
         *
         * @param resourcegroup String resourcegroup
         * @throws PulsarAdminException.NotAuthorizedException You don't have admin permission
         * @throws PulsarAdminException.NotFoundException      Resourcegroup does not exist
         * @throws PulsarAdminException                        Unexpected error
         * @see ResourceGroup
         * <p>
         * *
         */
        ResourceGroup getResourceGroup(String resourcegroup) throws PulsarAdminException;

        /**
         * Get policies for a namespace asynchronously.
         * <p/>
         * Get cnfiguration specified for a resourcegroup.
         * <p/>
         * Response example:
         *
         * <pre>
         * <code>
         *     "publishRateInMsgs" : "value",
         *     "PublishRateInBytes" : "value",
         *     "DispatchRateInMsgs" : "value",
         *     "DspatchRateInBytes" : "value"
         *     "ReplicationDispatchRateInMsgs" : "value"
         *     "ReplicationDispatchRateInBytes" : "value"
         * </code>
         * </pre>
         *
         * @param resourcegroup Namespace name
         * @see ResourceGroup
         */
        CompletableFuture<ResourceGroup> getResourceGroupAsync(String resourcegroup);

        /**
         * Create a new resourcegroup.
         * <p/>
         * Creates a new reourcegroup with the configuration specified.
         *
         * @param name       resourcegroup name
         * @param resourcegroup ResourceGroup configuration
         * @throws PulsarAdminException.NotAuthorizedException You don't have admin permission
         * @throws PulsarAdminException.ConflictException      Resourcegroup already exists
         * @throws PulsarAdminException                        Unexpected error
         */
        void createResourceGroup(String name, ResourceGroup resourcegroup) throws PulsarAdminException;

        /**
         * Create a new resourcegroup.
         * <p/>
         * Creates a new resourcegroup with the configuration specified.
         *
         * @param name           resourcegroup name
         * @param resourcegroup ResourceGroup configuration.
         */
        CompletableFuture<Void> createResourceGroupAsync(String name, ResourceGroup resourcegroup);

        /**
         * Update the configuration for a ResourceGroup.
         * <p/>
         * This operation requires Pulsar super-user privileges.
         *
         * @param name          resourcegroup name
         * @param resourcegroup resourcegroup configuration
         *
         * @throws PulsarAdminException.NotAuthorizedException
         *             Don't have admin permission
         * @throws PulsarAdminException.NotFoundException
         *             ResourceGroup does not exist
         * @throws PulsarAdminException
         *             Unexpected error
         */
        void updateResourceGroup(String name, ResourceGroup resourcegroup) throws PulsarAdminException;

        /**
         * Update the configuration for a ResourceGroup.
         * <p/>
         * This operation requires Pulsar super-user privileges.
         *
         * @param name          resourcegroup name
         * @param resourcegroup resourcegroup configuration
         */
        CompletableFuture<Void> updateResourceGroupAsync(String name, ResourceGroup resourcegroup);


        /**
         * Delete an existing resourcegroup.
         * <p/>
         * The resourcegroup needs to unused and not attached to any entity.
         *
         * @param resourcegroup Resourcegroup name
         * @throws PulsarAdminException.NotAuthorizedException You don't have admin permission
         * @throws PulsarAdminException.NotFoundException      Resourcegroup does not exist
         * @throws PulsarAdminException.ConflictException      Resourcegroup is in use
         * @throws PulsarAdminException                        Unexpected error
         */
        void deleteResourceGroup(String resourcegroup) throws PulsarAdminException;

        /**
         * Delete an existing resourcegroup.
         * <p/>
         * The resourcegroup needs to unused and not attached to any entity.
         *
         * @param resourcegroup Resourcegroup name
         */

        CompletableFuture<Void> deleteResourceGroupAsync(String resourcegroup);

}