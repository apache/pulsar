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
package org.apache.pulsar.broker.namespace;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.pulsar.common.policies.data.Policies.LAST_BOUNDARY;

import java.io.IOException;
import java.util.List;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.LocalZooKeeperConnectionService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

/**
 * This class encapsulate some utility functions for <code>ServiceUnit</code> related <code>ZooKeeper</code> operations
 *
 *
 */
public final class ServiceUnitZkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NamespaceService.class);

    public static final String UTF8 = "UTF8";

    private static final ObjectMapper jsonMapper = ObjectMapperFactory.create();

    /**
     * <code>ZooKeeper</code> root path for namespace ownership info
     */
    public static final String OWNER_INFO_ROOT = LocalZooKeeperCacheService.OWNER_INFO_ROOT;

    public static final String path(NamespaceBundle suname) {
        // The ephemeral node path for new namespaces should always have bundle name appended
        return OWNER_INFO_ROOT + "/" + suname.toString();
    }

    public static final NamespaceBundle suBundleFromPath(String path, NamespaceBundleFactory factory) {
        String[] parts = path.split("/");
        checkArgument(parts.length > 2);
        checkArgument(parts[1].equals("namespace"));
        checkArgument(parts.length > 4);

        if (parts.length > 5) {
            // this is a V1 path prop/cluster/namespace/hash
            Range<Long> range = getHashRange(parts[5]);
            return factory.getBundle(NamespaceName.get(parts[2], parts[3], parts[4]), range);
        } else {
            // this is a V2 path prop/namespace/hash
            Range<Long> range = getHashRange(parts[4]);
            return factory.getBundle(NamespaceName.get(parts[2], parts[3]), range);
        }
    }

    private static Range<Long> getHashRange(String rangePathPart) {
        String[] endPoints = rangePathPart.split("_");
        checkArgument(endPoints.length == 2, "Malformed bundle hash range path part:" + rangePathPart);
        Long startLong = Long.decode(endPoints[0]);
        Long endLong = Long.decode(endPoints[1]);
        BoundType endType = (endPoints[1].equals(LAST_BOUNDARY)) ? BoundType.CLOSED : BoundType.OPEN;
        return Range.range(startLong, BoundType.CLOSED, endLong, endType);
    }

    /**
     * initZK is only called when the NamespaceService is initialized. So, no need for synchronization.
     *
     * @return
     *
     * @throws PulsarServerException
     */
    public static final void initZK(ZooKeeper zkc, String selfBrokerUrl) {
        // initialize the zk client with values
        try {
            // check and create /namespace path
            LocalZooKeeperConnectionService.checkAndCreatePersistNode(zkc, OWNER_INFO_ROOT);
            // make sure to cleanup all remaining ephemeral nodes that shows ownership of this broker
            cleanupNamespaceNodes(zkc, OWNER_INFO_ROOT, selfBrokerUrl);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * cleanupNamespaceNodes is only called when the NamespaceService is initialized. So, no need for synchronization.
     *
     * @throws Exception
     */
    private static final void cleanupNamespaceNodes(ZooKeeper zkc, String root, String selfBrokerUrl) throws Exception {
        // we don't need a watch here since we are only cleaning up the stale ephemeral nodes from previous session
        try {
            for (String node : zkc.getChildren(root, false)) {
                String currentPath = root + "/" + node;
                // retrieve the content and try to decode with ServiceLookupData
                List<String> children = zkc.getChildren(currentPath, false);
                if (children.size() == 0) {
                    // clean up a single namespace node
                    cleanupSingleNamespaceNode(zkc, currentPath, selfBrokerUrl);
                } else {
                    // this is an intermediate node, which means this is v2 namespace path
                    cleanupNamespaceNodes(zkc, currentPath, selfBrokerUrl);
                }
            }
        } catch (NoNodeException nne) {
            LOG.info("No children for [{}]", nne.getPath());
        }
    }

    /**
     * cleanupSingleNamespaceNode is only called when the NamespaceService is initialized. So, no need for
     * synchronization.
     *
     * @throws Exception
     */
    private static final void cleanupSingleNamespaceNode(ZooKeeper zkc, String path, String selfBrokerUrl)
            throws Exception {
        String brokerUrl = null;
        try {
            byte[] data = zkc.getData(path, false, null);
            if (data == null || data.length == 0) {
                // skip, ephemeral node will not have zero byte
                return;
            }

            NamespaceEphemeralData zdata = jsonMapper.readValue(data, NamespaceEphemeralData.class);
            brokerUrl = zdata.getNativeUrl();

            if (selfBrokerUrl.equals(brokerUrl)) {
                // The znode indicate that the owner is this instance while this instance was just started before
                // acquiring any namespaces yet
                // Hence, the znode must have been created previously by this instance and needs to be cleaned up
                zkc.delete(path, -1);
            }
        } catch (NoNodeException nne) {
            // The node may expired before this instance tries to get or delete it from ZK. It is OK and considered as
            // NOOP.
        }
    }

    /**
     * Create name space Ephemeral node.
     *
     * @param zkc
     *            the <code>ZooKeeper</code> connected session object
     * @param nsname
     *            the name space name
     * @param value
     *            the broker url that serves the name space.
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     * @throws JsonMappingException
     * @throws JsonGenerationException
     */
    public static final NamespaceEphemeralData acquireNameSpace(ZooKeeper zkc, String path,
            NamespaceEphemeralData value)
            throws KeeperException, InterruptedException, JsonGenerationException, JsonMappingException, IOException {

        // the znode data to be written
        String data = jsonMapper.writeValueAsString(value);

        createZnodeOptimistic(zkc, path, data, CreateMode.EPHEMERAL);

        return value;
    }

    public static final BundlesData createBundlesIfAbsent(ZooKeeper zkc, String path, BundlesData initialBundles)
            throws JsonGenerationException, JsonMappingException, IOException, KeeperException, InterruptedException {
        String data = jsonMapper.writeValueAsString(initialBundles);

        createZnodeOptimistic(zkc, path, data, CreateMode.PERSISTENT);

        return initialBundles;
    }

    private static void createZnodeOptimistic(ZooKeeper zkc, String path, String data, CreateMode mode)
            throws KeeperException, InterruptedException {
        try {
            // create node optimistically
            checkNotNull(LocalZooKeeperConnectionService.createIfAbsent(zkc, path, data, mode));
        } catch (NoNodeException e) {
            // if path contains multiple levels after the root, create the intermediate nodes first
            String[] parts = path.split("/");
            if (parts.length > 3) {
                String int_path = path.substring(0, path.lastIndexOf("/"));
                if (zkc.exists(int_path, false) == null) {
                    // create the intermediate nodes
                    try {
                        ZkUtils.createFullPathOptimistic(zkc, int_path, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException nee) {
                        LOG.debug(
                                "Other broker preempted the full intermediate path [{}] already. Continue for acquiring the leaf ephemeral node.",
                                int_path);
                    }
                }
                checkNotNull(LocalZooKeeperConnectionService.createIfAbsent(zkc, path, data, mode));
            } else {
                // If failed to create immediate child of root node, throw exception
                throw e;
            }
        }
    }
}
