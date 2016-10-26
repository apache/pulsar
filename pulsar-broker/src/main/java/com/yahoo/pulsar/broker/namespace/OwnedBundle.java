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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.broker.PulsarService;

public class OwnedBundle {
    private static final Logger LOG = LoggerFactory.getLogger(OwnedBundle.class);

    private final NamespaceBundle bundle;

    /**
     * {@link #nsLock} is used to protect read/write access to {@link #active} flag and the corresponding code section
     * based on {@link #active} flag
     */
    private final ReentrantReadWriteLock nsLock = new ReentrantReadWriteLock();
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    /**
     * constructor
     *
     * @param nsname
     */
    public OwnedBundle(NamespaceBundle suName) {
        this.bundle = suName;
    };

    /**
     * Constructor to allow set initial active flag
     *
     * @param nsname
     * @param nssvc
     * @param active
     */
    public OwnedBundle(NamespaceBundle suName, boolean active) {
        this.bundle = suName;
        this.isActive.set(active);
    }

    /**
     * Access to the namespace name
     *
     * @return NamespaceName
     */
    public NamespaceBundle getNamespaceBundle() {
        return this.bundle;
    }

    /**
     * This method initiates the unload namespace process. It is invoked by Admin API
     * <code>Namespaces.unloadNamespace()</code>.
     *
     * @param pulsar
     * @param adminView
     * @param ownershipCache
     *
     * @throws Exception
     */
    public void handleUnloadRequest(PulsarService pulsar) throws Exception {

        long unloadBundleStartTime = System.nanoTime();
        // Need a per namespace RenetrantReadWriteLock
        // Here to do a writeLock to set the flag and proceed to check and close connections
        while (!this.nsLock.writeLock().tryLock(1, TimeUnit.SECONDS)) {
            // Using tryLock to avoid deadlocks caused by 2 threads trying to acquire 2 readlocks (eg: JMS replicators)
            // while a handleUnloadRequest happens in the middle
            LOG.warn("Contention on OwnedBundle rw lock. Retrying to acquire lock write lock");
        }

        try {
            // set the flag locally s.t. no more producer/consumer to this namespace is allowed
            if (!this.isActive.compareAndSet(true, false)) {
                // An exception is thrown when the namespace is not in active state (i.e. another thread is
                // removing/have removed it)
                throw new IllegalStateException(
                        "Namespace is not active. ns:" + this.bundle + "; state:" + this.isActive.get());
            }
        } finally {
            // no matter success or not, unlock
            this.nsLock.writeLock().unlock();
        }

        int unloadedTopics = 0;
        try {
            LOG.info("Disabling ownership: {}", this.bundle);
            pulsar.getNamespaceService().getOwnershipCache().disableOwnership(this.bundle);

            // Handle unload of persistent topics
            unloadedTopics = pulsar.getBrokerService().unloadServiceUnit(bundle).get();
            pulsar.getNamespaceService().getOwnershipCache().removeOwnership(bundle);
        } catch (Exception e) {
            LOG.error(String.format("failed to unload a namespace. ns=%s", bundle.toString()), e);
            throw new RuntimeException(e);
        }

        double unloadBundleTime = TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - unloadBundleStartTime));
        LOG.info("Unloading {} namespace-bundle with {} topics completed in {} ms", this.bundle, unloadedTopics, unloadBundleTime);
    }

    /**
     * Access method to the namespace state to check whether the namespace is active or not
     *
     * @return boolean value indicate that the namespace is active or not.
     */
    public boolean isActive() {
        return this.isActive.get();
    }

    public void setActive(boolean active) {
        isActive.set(active);
    }
}
