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
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.common.policies.data.TenantInfo;

/**
 * Admin interface for tenants management.
 */
public interface Tenants {
    /**
     * Get the list of tenants.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>["my-tenant", "other-tenant", "third-tenant"]</code>
     * </pre>
     *
     * @return the list of Pulsar tenants
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getTenants() throws PulsarAdminException;

    /**
     * Get the list of tenants asynchronously.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>["my-tenant", "other-tenant", "third-tenant"]</code>
     * </pre>
     *
     * @return the list of Pulsar tenants
     */
    CompletableFuture<List<String>> getTenantsAsync();

    /**
     * Get the config of the tenant.
     * <p/>
     * Get the admin configuration for a given tenant.
     *
     * @param tenant
     *            Tenant name
     * @return the tenant configuration
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Tenant does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    TenantInfo getTenantInfo(String tenant) throws PulsarAdminException;

    /**
     * Get the config of the tenant asynchronously.
     * <p/>
     * Get the admin configuration for a given tenant.
     *
     * @param tenant
     *            Tenant name
     * @return the tenant configuration
     */
    CompletableFuture<TenantInfo> getTenantInfoAsync(String tenant);

    /**
     * Create a new tenant.
     * <p/>
     * Provisions a new tenant. This operation requires Pulsar super-user privileges.
     *
     * @param tenant
     *            Tenant name
     * @param config
     *            Config data
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws ConflictException
     *             Tenant already exists
     * @throws PreconditionFailedException
     *             Tenant name is not valid
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createTenant(String tenant, TenantInfo config) throws PulsarAdminException;

    /**
     * Create a new tenant asynchronously.
     * <p/>
     * Provisions a new tenant. This operation requires Pulsar super-user privileges.
     *
     * @param tenant
     *            Tenant name
     * @param config
     *            Config data
     */
    CompletableFuture<Void> createTenantAsync(String tenant, TenantInfo config);

    /**
     * Update the admins for a tenant.
     * <p/>
     * This operation requires Pulsar super-user privileges.
     *
     * @param tenant
     *            Tenant name
     * @param config
     *            Config data
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Tenant does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateTenant(String tenant, TenantInfo config) throws PulsarAdminException;

    /**
     * Update the admins for a tenant asynchronously.
     * <p/>
     * This operation requires Pulsar super-user privileges.
     *
     * @param tenant
     *            Tenant name
     * @param config
     *            Config data
     */
    CompletableFuture<Void> updateTenantAsync(String tenant, TenantInfo config);

    /**
     * Delete an existing tenant.
     * <p/>
     * Delete a tenant and all namespaces and topics under it.
     *
     * @param tenant
     *            Tenant name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             The tenant does not exist
     * @throws ConflictException
     *             The tenant still has active namespaces
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteTenant(String tenant) throws PulsarAdminException;

    /**
     * Delete an existing tenant.
     * <p/>
     * Force flag delete a tenant forcefully and all namespaces and topics under it.
     *
     * @param tenant
     *            Tenant name
     * @param force
     *            Delete tenant forcefully
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             The tenant does not exist
     * @throws ConflictException
     *             The tenant still has active namespaces
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteTenant(String tenant, boolean force) throws PulsarAdminException;

    /**
     * Delete an existing tenant asynchronously.
     * <p/>
     * Delete a tenant and all namespaces and topics under it.
     *
     * @param tenant
     *            Tenant name
     */
    CompletableFuture<Void> deleteTenantAsync(String tenant);

    /**
     * Delete an existing tenant asynchronously.
     * <p/>
     * Force flag delete a tenant forcefully and all namespaces and topics under it.
     *
     * @param tenant
     *            Tenant name
     * @param force
     *            Delete tenant forcefully
     */
    CompletableFuture<Void> deleteTenantAsync(String tenant, boolean force);
}
