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
package org.apache.pulsar.metadata.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.Test;

public class ZKMetadataStoreTest extends BaseMetadataStoreTest {
    @Test
    public void testOperationTimeout() {
        ZooKeeper zooKeeper = spy(zkc);
        doNothing().when(zooKeeper).multi(any(), any(), any());

        MetadataStore zkMetadataStore = new ZKMetadataStore(zooKeeper);
        CompletionException ex = assertThrows(CompletionException.class, () -> zkMetadataStore.get("/").join());
        assertEquals(TimeoutException.class, ex.getCause().getClass());
    }
}
