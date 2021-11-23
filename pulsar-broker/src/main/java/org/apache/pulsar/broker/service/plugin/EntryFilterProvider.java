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
package org.apache.pulsar.broker.service.plugin;


import static com.google.common.base.Preconditions.checkArgument;
import java.io.IOException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;

public interface EntryFilterProvider {
    /**
     *  Use `EntryFilterProvider` to create `EntryFilter` through reflection
     * @param subscription
     * @return
     */
    EntryFilter createEntriesFilter(Subscription subscription) throws IOException;

    /**
     * The default implementation class of EntriesFilterProvider
     */
    class DefaultEntryFilterProviderImpl implements EntryFilterProvider {
        private final ServiceConfiguration serviceConfiguration;
        private final Subscription subscription;

        public DefaultEntryFilterProviderImpl(ServiceConfiguration serviceConfiguration, Subscription subscription) {
            this.serviceConfiguration = serviceConfiguration;
            this.subscription = subscription;
        }

        @Override
        public EntryFilter createEntriesFilter(Subscription subscription) throws IOException {
            Class<?> providerClass;
            try {
                providerClass = Class.forName(serviceConfiguration.getEntryFilterClassName());
                Object obj = providerClass.getDeclaredConstructor().newInstance();
                checkArgument(obj instanceof EntryFilter,
                        "The instance is not an instance of "
                                + serviceConfiguration.getEntryFilterClassName());
                return (EntryFilter) obj;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

}
