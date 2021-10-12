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
package org.apache.pulsar.broker.transaction.pendingack;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;

/**
 * Provider of transaction pending ack store.
 */
public interface TransactionPendingAckStoreProvider {

    /**
     * Construct a provider from the provided class.
     *
     * @param providerClassName {@link String} the provider class name
     * @return an instance of transaction buffer provider.
     */
    static TransactionPendingAckStoreProvider newProvider(String providerClassName) throws IOException {
        Class<?> providerClass;
        try {
            providerClass = Class.forName(providerClassName);
            Object obj = providerClass.getDeclaredConstructor().newInstance();
            checkArgument(obj instanceof TransactionPendingAckStoreProvider,
                    "The factory has to be an instance of "
                            + TransactionPendingAckStoreProvider.class.getName());

            return (TransactionPendingAckStoreProvider) obj;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Open the pending ack store.
     *
     * @param subscription {@link PersistentSubscription}
     * @return a future represents the result of the operation.
     *         an instance of {@link PendingAckStore} is returned
     *         if the operation succeeds.
     */
    CompletableFuture<PendingAckStore> newPendingAckStore(PersistentSubscription subscription);

    /**
     * Check pending ack store has been initialized before.
     *
     * @param subscription {@link PersistentSubscription}
     * @return a future represents the result of the operation.
     *         an instance of {@link Boolean} is returned
     *         if the operation succeeds.
     */
    CompletableFuture<Boolean> checkInitializedBefore(PersistentSubscription subscription);
}