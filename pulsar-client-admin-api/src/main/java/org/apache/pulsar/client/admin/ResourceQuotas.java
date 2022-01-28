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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.common.policies.data.ResourceQuota;

/**
 * Admin interface on interacting with resource quotas.
 */
public interface ResourceQuotas {

    /**
     * Get default resource quota for new resource bundles.
     * <p/>
     * Get default resource quota for new resource bundles.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>
     *  {
     *      "msgRateIn" : 10,
     *      "msgRateOut" : 30,
     *      "bandwidthIn" : 10000,
     *      "bandwidthOut" : 30000,
     *      "memory" : 100,
     *      "dynamic" : true
     *  }
     * </code>
     * </pre>
     *
     * @throws NotAuthorizedException
     *             Permission denied
     * @throws PulsarAdminException
     *             Unexpected error
     */
    ResourceQuota getDefaultResourceQuota() throws PulsarAdminException;

    /**
     * Get default resource quota for new resource bundles asynchronously.
     * <p/>
     * Get default resource quota for new resource bundles.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>
     *  {
     *      "msgRateIn" : 10,
     *      "msgRateOut" : 30,
     *      "bandwidthIn" : 10000,
     *      "bandwidthOut" : 30000,
     *      "memory" : 100,
     *      "dynamic" : true
     *  }
     * </code>
     * </pre>
     *
     */
    CompletableFuture<ResourceQuota> getDefaultResourceQuotaAsync();

    /**
     * Set default resource quota for new namespace bundles.
     * <p/>
     * Set default resource quota for new namespace bundles.
     * <p/>
     * The resource quota can be set with these properties:
     * <ul>
     * <li><code>msgRateIn</code> : The maximum incoming messages per second.
     * <li><code>msgRateOut</code> : The maximum outgoing messages per second.
     * <li><code>bandwidthIn</code> : The maximum inbound bandwidth used.
     * <li><code>bandwidthOut</code> : The maximum outbound bandwidth used.
     * <li><code>memory</code> : The maximum memory used.
     * <li><code>dynamic</code> : allow the quota to be dynamically re-calculated.
     * </li>
     * </ul>
     *
     * <p/>
     * Request parameter example:
     *
     * <pre>
     * <code>
     *  {
     *      "msgRateIn" : 10,
     *      "msgRateOut" : 30,
     *      "bandwidthIn" : 10000,
     *      "bandwidthOut" : 30000,
     *      "memory" : 100,
     *      "dynamic" : false
     *  }
     * </code>
     * </pre>
     *
     * @param quota
     *             The new ResourceQuota
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setDefaultResourceQuota(ResourceQuota quota) throws PulsarAdminException;

    /**
     * Set default resource quota for new namespace bundles asynchronously.
     * <p/>
     * Set default resource quota for new namespace bundles.
     * <p/>
     * The resource quota can be set with these properties:
     * <ul>
     * <li><code>msgRateIn</code> : The maximum incoming messages per second.
     * <li><code>msgRateOut</code> : The maximum outgoing messages per second.
     * <li><code>bandwidthIn</code> : The maximum inbound bandwidth used.
     * <li><code>bandwidthOut</code> : The maximum outbound bandwidth used.
     * <li><code>memory</code> : The maximum memory used.
     * <li><code>dynamic</code> : allow the quota to be dynamically re-calculated.
     * </li>
     * </ul>
     *
     * <p/>
     * Request parameter example:
     *
     * <pre>
     * <code>
     *  {
     *      "msgRateIn" : 10,
     *      "msgRateOut" : 30,
     *      "bandwidthIn" : 10000,
     *      "bandwidthOut" : 30000,
     *      "memory" : 100,
     *      "dynamic" : false
     *  }
     * </code>
     * </pre>
     *
     * @param quota
     *             The new ResourceQuota
     */
    CompletableFuture<Void> setDefaultResourceQuotaAsync(ResourceQuota quota);

    /**
     * Get resource quota of a namespace bundle.
     * <p/>
     * Get resource quota of a namespace bundle.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>
     *  {
     *      "msgRateIn" : 10,
     *      "msgRateOut" : 30,
     *      "bandwidthIn" : 10000,
     *      "bandwidthOut" : 30000,
     *      "memory" : 100,
     *      "dynamic" : true
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *             Namespace name
     * @param bundle
     *             Range of bundle {start}_{end}
     *
     * @throws NotAuthorizedException
     *             Permission denied
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    ResourceQuota getNamespaceBundleResourceQuota(String namespace, String bundle) throws PulsarAdminException;

    /**
     * Get resource quota of a namespace bundle asynchronously.
     * <p/>
     * Get resource quota of a namespace bundle.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>
     *  {
     *      "msgRateIn" : 10,
     *      "msgRateOut" : 30,
     *      "bandwidthIn" : 10000,
     *      "bandwidthOut" : 30000,
     *      "memory" : 100,
     *      "dynamic" : true
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *             Namespace name
     * @param bundle
     *             Range of bundle {start}_{end}
     *
     */
    CompletableFuture<ResourceQuota> getNamespaceBundleResourceQuotaAsync(String namespace, String bundle);

    /**
     * Set resource quota for a namespace bundle.
     * <p/>
     * Set resource quota for a namespace bundle.
     * <p/>
     * The resource quota can be set with these properties:
     * <ul>
     * <li><code>msgRateIn</code> : The maximum incoming messages per second.
     * <li><code>msgRateOut</code> : The maximum outgoing messages per second.
     * <li><code>bandwidthIn</code> : The maximum inbound bandwidth used.
     * <li><code>bandwidthOut</code> : The maximum outbound bandwidth used.
     * <li><code>memory</code> : The maximum memory used.
     * <li><code>dynamic</code> : allow the quota to be dynamically re-calculated.
     * </li>
     * </ul>
     *
     * <p/>
     * Request parameter example:
     *
     * <pre>
     * <code>
     *  {
     *      "msgRateIn" : 10,
     *      "msgRateOut" : 30,
     *      "bandwidthIn" : 10000,
     *      "bandwidthOut" : 30000,
     *      "memory" : 100,
     *      "dynamic" : false
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *             Namespace name
     * @param bundle
     *             Bundle range {start}_{end}
     * @param quota
     *             The new ResourceQuota
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setNamespaceBundleResourceQuota(String namespace, String bundle, ResourceQuota quota)
            throws PulsarAdminException;

    /**
     * Set resource quota for a namespace bundle asynchronously.
     * <p/>
     * Set resource quota for a namespace bundle.
     * <p/>
     * The resource quota can be set with these properties:
     * <ul>
     * <li><code>msgRateIn</code> : The maximum incoming messages per second.
     * <li><code>msgRateOut</code> : The maximum outgoing messages per second.
     * <li><code>bandwidthIn</code> : The maximum inbound bandwidth used.
     * <li><code>bandwidthOut</code> : The maximum outbound bandwidth used.
     * <li><code>memory</code> : The maximum memory used.
     * <li><code>dynamic</code> : allow the quota to be dynamically re-calculated.
     * </li>
     * </ul>
     *
     * <p/>
     * Request parameter example:
     *
     * <pre>
     * <code>
     *  {
     *      "msgRateIn" : 10,
     *      "msgRateOut" : 30,
     *      "bandwidthIn" : 10000,
     *      "bandwidthOut" : 30000,
     *      "memory" : 100,
     *      "dynamic" : false
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *             Namespace name
     * @param bundle
     *             Bundle range {start}_{end}
     * @param quota
     *             The new ResourceQuota
     *
     */
    CompletableFuture<Void> setNamespaceBundleResourceQuotaAsync(String namespace, String bundle, ResourceQuota quota);

    /**
     * Reset resource quota for a namespace bundle to default value.
     * <p/>
     * Reset resource quota for a namespace bundle to default value.
     * <p/>
     * The resource quota policy will fall back to the default.
     *
     * @param namespace
     *             Namespace name
     * @param bundle
     *             Bundle range {start}_{end}
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void resetNamespaceBundleResourceQuota(String namespace, String bundle) throws PulsarAdminException;

    /**
     * Reset resource quota for a namespace bundle to default value asynchronously.
     * <p/>
     * Reset resource quota for a namespace bundle to default value.
     * <p/>
     * The resource quota policy will fall back to the default.
     *
     * @param namespace
     *             Namespace name
     * @param bundle
     *             Bundle range {start}_{end}
     *
     */
    CompletableFuture<Void> resetNamespaceBundleResourceQuotaAsync(String namespace, String bundle);
}

