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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.testng.annotations.Test;

public class ZKSessionTest extends BaseMetadataStoreTest {

    @Test
    public void testDisconnection() throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis(30_000)
                        .build());

        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);

        zks.stop();

        SessionEvent e = sessionEvents.poll(5, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.ConnectionLost);

        zks.start();
        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.Reconnected);

        e = sessionEvents.poll(1, TimeUnit.SECONDS);
        assertNull(e);
    }

    @Test
    public void testSessionLost() throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(zks.getConnectionString(),
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis(10_000)
                        .build());

        BlockingQueue<SessionEvent> sessionEvents = new LinkedBlockingQueue<>();
        store.registerSessionListener(sessionEvents::add);

        zks.stop();

        SessionEvent e = sessionEvents.poll(5, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.ConnectionLost);

        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.SessionLost);

        zks.start();
        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.Reconnected);
        e = sessionEvents.poll(10, TimeUnit.SECONDS);
        assertEquals(e, SessionEvent.SessionReestablished);

        e = sessionEvents.poll(1, TimeUnit.SECONDS);
        assertNull(e);
    }
}
