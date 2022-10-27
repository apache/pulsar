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
package org.apache.pulsar.broker.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceBundle;

public class CanPausedNamespaceService extends NamespaceService {

    private volatile boolean paused = false;

    private final PulsarService pulsar;

    private ReentrantLock lock = new ReentrantLock();

    private List<CompletableFuture<Void>> runningEventListeners = Collections.synchronizedList(new ArrayList<>());

    public CanPausedNamespaceService(PulsarService pulsar) {
        super(pulsar);
        this.pulsar = pulsar;
    }

    @Override
    protected void onNamespaceBundleOwned(NamespaceBundle bundle) {
        lock.lock();
        try {
            if (paused){
                return;
            }
            // Manually calling the method "addOwnedNamespaceBundleAsync" to get the future object, but the bundle
            // counter for that ns will +1, so "removeOwnedNamespaceBundleAsync" will later be triggered manually
            // to make the counter -1.
            CompletableFuture<Void> readerCreateTask =
                    pulsar.getTopicPoliciesService().addOwnedNamespaceBundleAsync(bundle);
            super.onNamespaceBundleOwned(bundle);
            readerCreateTask.whenComplete((ignore, ex) -> {
                // See above.
                pulsar.getTopicPoliciesService().removeOwnedNamespaceBundleAsync(bundle);
            });
            runningEventListeners.add(readerCreateTask);
        } finally {
            lock.unlock();
        }
    }

    public List<CompletableFuture<Void>> pause(){
        lock.lock();
        try {
            paused = true;
            return runningEventListeners;
        } finally {
            lock.unlock();
        }
    }

    public void resume(){
        lock.lock();
        try {
            paused = false;
        } finally {
            lock.unlock();
        }
    }
}
