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
package org.apache.pulsar.broker.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache service and ZooKeeper read/write access for resource quota.
 */
public class ResourceQuotaCache {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceQuotaCache.class);

    // Root path for resource-quota
    public static final String RESOURCE_QUOTA_ROOT = "/loadbalance/resource-quota";
    // Serialize/de-serialize JSON objects
    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();
    // Read only cache
    private final ZooKeeperDataCache<ResourceQuota> resourceQuotaCache;
    // Local zookeeper cache & connection
    private final ZooKeeperCache localZkCache;
    // Defailt initial quota
    private static final ResourceQuota initialQuota;
    static {
        ResourceQuota quota = new ResourceQuota();
        // incoming msg / sec
        quota.setMsgRateIn(40);
        // outgoing msg / sec
        quota.setMsgRateOut(120);
        // incoming bytes / sec
        quota.setBandwidthIn(100000);
        // outgoing bytes / sec
        quota.setBandwidthOut(300000);
        // Mbytes
        quota.setMemory(80);
        // allow dynamically re-calculating
        quota.setDynamic(true);
        initialQuota = quota;
    }

    // generate path for a specified ServiceUnit, return root path if suName is null or empty
    private static String path(String suName) {
        if (suName != null && !suName.isEmpty()) {
            return RESOURCE_QUOTA_ROOT + "/namespace/" + suName;
        } else {
            return RESOURCE_QUOTA_ROOT + "/default";
        }
    }

    public ResourceQuotaCache(ZooKeeperCache cache) {
        this.localZkCache = cache;
        this.resourceQuotaCache = new ZooKeeperDataCache<ResourceQuota>(cache) {
            @Override
            public ResourceQuota deserialize(String path, byte[] content) throws Exception {
                return jsonMapper.readValue(content, ResourceQuota.class);
            }
        };
    }

    public static ResourceQuota getInitialQuotaValue() {
        return ResourceQuotaCache.initialQuota;
    }

    private ResourceQuota readQuotaFromZnode(String zpath) {
        try {
            return this.resourceQuotaCache.get(zpath).orElseGet(() -> new ResourceQuota());
        } catch (Exception e) {
            LOG.warn("Failed to read quota from znode {}: {}", zpath, e);
            return new ResourceQuota();
        }
    }

    private void saveQuotaToZnode(String zpath, ResourceQuota quota) throws Exception {
        ZooKeeper zk = this.localZkCache.getZooKeeper();
        if (zk.exists(zpath, false) == null) {
            try {
                ZkUtils.createFullPathOptimistic(zk, zpath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
            }
        }
        zk.setData(zpath, this.jsonMapper.writeValueAsBytes(quota), -1);
    }

    /**
     * Initialize the resource quota root node in ZooKeeper.
     *
     */
    public void initZK() throws PulsarServerException {
        String zpath = ResourceQuotaCache.path(null);
        ResourceQuota quota = this.readQuotaFromZnode(zpath);
        if (!quota.isValid()) {
            quota = ResourceQuotaCache.getInitialQuotaValue();
            try {
                this.saveQuotaToZnode(zpath, quota);
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }
    }

    /**
     * Get resource quota for a specified service unit.
     *
     * @param suName
     *            identifier of the <code>ServiceUnit</code>
     *
     * @return the <code>ResourceQuota</code>.
     */
    public ResourceQuota getQuota(String suName) {
        String zpath = ResourceQuotaCache.path(suName);
        ResourceQuota quota = this.readQuotaFromZnode(zpath);
        if (!quota.isValid()) {
            String defaultZpath = ResourceQuotaCache.path(null);
            quota = this.readQuotaFromZnode(defaultZpath);
            if (!quota.isValid()) {
                quota = ResourceQuotaCache.getInitialQuotaValue();
            }
        }

        return quota;
    }

    /**
     * Get resource quota for a specified service unit.
     *
     * @param suName
     *            identifier of the <code>ServiceUnit</code>
     *
     * @return the <code>ResourceQuota</code>.
     */
    public ResourceQuota getQuota(NamespaceBundle suName) {
        String suNameStr = (suName == null) ? null : suName.toString();
        return this.getQuota(suNameStr);
    }

    /**
     * Get default resource quota..
     *
     * @return the default <code>ResourceQuota</code>.
     */
    public ResourceQuota getDefaultQuota() {
        return this.getQuota((String) null);
    }

    /**
     * Set resource quota for a specified <code>ServiceUnit</code>.
     *
     * @param suName
     *            identifier of the <code>ServiceUnit</code>
     * @param quota
     *            <code>ResourceQuota</code> to set.
     */
    public void setQuota(String suName, ResourceQuota quota) throws Exception {
        String zpath = ResourceQuotaCache.path(suName);
        this.resourceQuotaCache.invalidate(zpath);
        this.saveQuotaToZnode(zpath, quota);
    }

    /**
     * Set resource quota for a specified <code>ServiceUnit</code>.
     *
     * @param suName
     *            identifier of the <code>ServiceUnit</code>
     * @param quota
     *            <code>ResourceQuota</code> to set.
     */
    public void setQuota(NamespaceBundle suName, ResourceQuota quota) throws Exception {
        String suNameStr = (suName == null) ? null : suName.toString();
        this.setQuota(suNameStr, quota);
    }

    /**
     * Set default resource quota..
     *
     * @param quota
     *            <code>ResourceQuota</code> to set.
     */
    public void setDefaultQuota(ResourceQuota quota) throws Exception {
        this.setQuota((String) null, quota);
    }

    /**
     * Remove resource quota for a specified <code>ServiceUnit</code> to use the default quota.
     *
     * @param suName
     *            identifier of the <code>ServiceUnit</code>
     */
    public void unsetQuota(String suName) throws Exception {
        this.setQuota(suName, new ResourceQuota());
    }

    /**
     * Remove resource quota for a specified <code>ServiceUnit</code> to use the default quota.
     *
     * @param suName
     *            identifier of the <code>ServiceUnit</code>
     */
    public void unsetQuota(NamespaceBundle suName) throws Exception {
        String suNameStr = (suName == null) ? null : suName.toString();
        this.unsetQuota(suNameStr);
    }
}
