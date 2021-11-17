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
package org.apache.pulsar.functions.instance.state;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.*;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link BKStateStoreImpl}.
 */
public class PulsarMetadataStateStoreImplTest {

    private static final String TENANT = "test-tenant";
    private static final String NS = "test-ns";
    private static final String NAME = "test-name";
    private static final String FQSN = "test-tenant/test-ns/test-name";
    private static final String PREFIX = "/prefix";
    private static final String PREFIX_PATH = PREFIX + '/' + FQSN + '/';

    private MetadataStore store;
    private MetadataCache<Long> countersCache;
    private DefaultStateStore stateContext;

    @BeforeMethod
    public void setup() throws Exception {
        this.store = MetadataStoreFactory.create("memory://local", MetadataStoreConfig.builder().build());
        this.countersCache = store.getMetadataCache(Long.class);
        this.stateContext = new PulsarMetadataStateStoreImpl(store, "/prefix", TENANT, NS, NAME);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        this.store.close();
    }

    @Test
    public void testGetter() {
        assertEquals(stateContext.tenant(), TENANT);
        assertEquals(stateContext.namespace(), NS);
        assertEquals(stateContext.name(), NAME);
        assertEquals(stateContext.fqsn(), FQSN);
    }

    @Test
    public void testIncr() throws Exception {
        stateContext.incrCounter("test-key", 10L);
        assertEquals(countersCache.get(PREFIX_PATH + "test-key").join().get().longValue(), 10);
    }

    @Test
    public void testPut() throws Exception {
        stateContext.put("test-key", ByteBuffer.wrap("test-value".getBytes(UTF_8)));
        assertEquals(store.get(PREFIX_PATH + "test-key").join().get().getValue(), "test-value".getBytes(UTF_8));
    }

    @Test
    public void testDelete() throws Exception {
        stateContext.put("test-key", ByteBuffer.wrap("test-value".getBytes(UTF_8)));
        assertEquals("test-value".getBytes(UTF_8), store.get(PREFIX_PATH + "test-key").join().get().getValue());
        stateContext.delete("test-key");
        assertFalse(store.get(PREFIX_PATH + "test-key").join().isPresent());
    }

    @Test
    public void testGetAmount() throws Exception {
        assertEquals(stateContext.getCounter("test-key"), 0);
        stateContext.incrCounter("test-key", 10L);
        assertEquals(countersCache.get(PREFIX_PATH + "test-key").join().get().longValue(), 10);
        assertEquals(stateContext.getCounter("test-key"), 10);
    }

    @Test
    public void testGetKeyNotPresent() throws Exception {
        CompletableFuture<ByteBuffer> result = stateContext.getAsync("test-key");
        assertTrue(result != null);
        assertEquals(result.get(), null);
    }

}
