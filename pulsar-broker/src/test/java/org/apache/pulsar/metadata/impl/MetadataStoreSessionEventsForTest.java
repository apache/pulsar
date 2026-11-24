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
package org.apache.pulsar.metadata.impl;

import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

/**
 * Delivers a session event to the listeners of a broker's metadata store in tests, as the store does when its
 * session state changes.
 */
public final class MetadataStoreSessionEventsForTest {

    private MetadataStoreSessionEventsForTest() {
    }

    /**
     * Delivers the event to the store's session listeners. For a {@link DualMetadataStore}, the event is delivered by
     * its source store, whose session events reach the listeners registered with the dual store.
     */
    public static void send(MetadataStore store, SessionEvent event) {
        MetadataStore target = store instanceof DualMetadataStore dual ? dual.sourceStore : store;
        if (!(target instanceof AbstractMetadataStore abstractStore)) {
            throw new IllegalArgumentException("Unsupported metadata store: " + target.getClass().getName());
        }
        abstractStore.receivedSessionEvent(event);
    }
}
