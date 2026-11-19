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
package org.apache.pulsar.metadata.impl.oxia;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.oxia.client.api.AsyncOxiaClient;
import org.apache.pulsar.metadata.api.MetadataEventSynchronizer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class OxiaMetadataStoreSynchronizerTest {

    private OxiaMetadataStore store;

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    @Test
    public void constructionRegistersSyncListenerExactlyOnce() {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        MetadataEventSynchronizer synchronizer = mock(MetadataEventSynchronizer.class);

        store = new OxiaMetadataStore(client, "test-identity", synchronizer);

        verify(synchronizer, times(1)).registerSyncListener(any());
        assertTrue(store.getMetadataEventSynchronizer().isPresent());
        assertSame(store.getMetadataEventSynchronizer().get(), synchronizer);
    }
}
