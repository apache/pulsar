/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.namespace;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.common.naming.NamespaceBundles;

/**
 * This class provides a cache service for all the service unit ownership among the brokers. It provide a cache service
 * as well as ZooKeeper read/write functions for a) lookup of a service unit ownership to a broker; b) take ownership of
 * a service unit by the local broker
 *
 *
 */
public interface NamespaceOwnershipCache {

    /**
     * Method to get the current owner of the <code>ServiceUnit</code>
     *
     * @param suId
     *            identifier of the <code>ServiceUnit</code>
     * @return The ephemeral node data showing the current ownership info in <code>ZooKeeper</code>
     * @throws Exception
     *             throws exception if no ownership info is found
     */
    CompletableFuture<Optional<NamespaceEphemeralData>> getOwnerAsync(NamespaceBundle suname);

    /**
     * Method to get the current owner of the <code>ServiceUnit</code> or set the local broker as the owner if absent
     *
     * @param suId
     *            identifier of the <code>NamespaceBundle</code>
     * @return The ephemeral node data showing the current ownership info in <code>ZooKeeper</code>
     * @throws Exception
     */
    CompletableFuture<NamespaceEphemeralData> tryAcquiringOwnership(NamespaceBundle bundle) throws Exception;

    /**
     * Method to remove the ownership of local broker on the <code>NamespaceBundle</code>, if owned
     *
     */
    CompletableFuture<Void> removeOwnership(NamespaceBundle bundle);

    /**
     * Method to remove ownership of all owned bundles
     *
     * @param bundles
     *            <code>NamespaceBundles</code> to remove from ownership cache
     */
    CompletableFuture<Void> removeOwnership(NamespaceBundles bundles);

    /**
     * Method to access the map of all <code>ServiceUnit</code> objects owned by the local broker
     *
     * @return a map of owned <code>ServiceUnit</code> objects
     */
    Map<String, OwnedBundle> getOwnedBundles();

    /**
     * Checked whether a particular bundle is currently owned by this broker
     *
     * @param bundle
     * @return
     */
    boolean isNamespaceBundleOwned(NamespaceBundle bundle);

    /**
     * Return the {@link OwnedBundle} instance from the local cache. Does not block.
     *
     * @param bundle
     * @return
     */
    OwnedBundle getOwnedBundle(NamespaceBundle bundle);

    /**
     * Disable bundle in local cache and on zk
     * 
     * @param bundle
     * @throws Exception
     */
    void disableOwnership(NamespaceBundle bundle) throws Exception;

    /**
     * Update bundle state in a local cache
     * 
     * @param bundle
     * @throws Exception
     */
    void updateBundleState(NamespaceBundle bundle, boolean isActive) throws Exception;

    /**
     * Get Self-namespaceData node
     * 
     * @return
     */
    NamespaceEphemeralData getSelfOwnerInfo();
}
