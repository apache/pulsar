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
package org.apache.pulsar.metadata.bookkeeper;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

public class PulsarMetadataClientDriver extends AbstractMetadataDriver implements MetadataClientDriver {

    // register myself to driver manager
    static {
        MetadataDrivers.registerClientDriver(METADATA_STORE_SCHEME, PulsarMetadataClientDriver.class);
    }

    @Override
    public MetadataClientDriver initialize(ClientConfiguration clientConfiguration,
                                           ScheduledExecutorService scheduledExecutorService,
                                           StatsLogger statsLogger,
                                           Optional<Object> optionalCtx) throws MetadataException {
        super.initialize(clientConfiguration);
        return this;
    }

    @Override
    public RegistrationClient getRegistrationClient() {
        return registrationClient;
    }

    @Override
    public LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
        return ledgerManagerFactory;
    }

    @Override
    public LayoutManager getLayoutManager() {
        return layoutManager;
    }

    @Override
    public void setSessionStateListener(SessionStateListener sessionStateListener) {
        store.registerSessionListener(event -> {
            if (event == SessionEvent.SessionLost) {
                sessionStateListener.onSessionExpired();
            }
        });
    }
}
