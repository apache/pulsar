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

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;

public class PulsarMetadataBookieDriver extends AbstractMetadataDriver implements MetadataBookieDriver {

    // register myself
    static {
        MetadataDrivers.registerBookieDriver(METADATA_STORE_SCHEME, PulsarMetadataBookieDriver.class);
    }

    @Override
    public MetadataBookieDriver initialize(ServerConfiguration serverConfiguration,
                                           RegistrationManager.RegistrationListener registrationListener,
                                           StatsLogger statsLogger) throws MetadataException {
        super.initialize(serverConfiguration);
        return this;
    }

    @Override
    public RegistrationManager getRegistrationManager() {
        return registrationManager;
    }

    @Override
    public LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
        return ledgerManagerFactory;
    }

    @Override
    public LayoutManager getLayoutManager() {
        return layoutManager;
    }
}
