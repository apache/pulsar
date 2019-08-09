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
package org.apache.bookkeeper.statelib.impl.kv;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import com.google.common.io.Files;
import java.io.File;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.ByteArrayCoder;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.impl.kv.RocksdbKVAsyncStore;
import org.apache.bookkeeper.statelib.impl.kv.RocksdbKVStore;
import org.apache.commons.io.FileUtils;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test of {@link RocksdbKVStore}.
 */
@Slf4j
public class TestRocksdbKVAsyncStore extends TestDistributedLogBase {

    public static URI uri;
    public static Namespace namespace;

    @BeforeClass
    public static void setupCluster() throws Exception {
        TestDistributedLogBase.setupCluster();
        uri = DLMTestUtil.createDLMURI(zkPort, "/mvcc");
        conf.setPeriodicFlushFrequencyMilliSeconds(2);
        conf.setWriteLockEnabled(false);
        namespace = NamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .clientId("test-mvcc-async-store")
                .build();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        if (null != namespace) {
            namespace.close();
        }
        TestDistributedLogBase.teardownCluster();
    }

    private String streamName;
    public File tempDir;
    public RocksdbKVAsyncStore<byte[], byte[]> store;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        ensureURICreated(uri);

        tempDir = Files.createTempDir();

        store = new RocksdbKVAsyncStore<>(
                () -> new RocksdbKVStore<>(),
                () -> namespace);
    }

    public StateStoreSpec initSpec(String streamName) {
        return StateStoreSpec.builder()
                .name(streamName)
                .keyCoder(ByteArrayCoder.of())
                .valCoder(ByteArrayCoder.of())
                .stream(streamName)
                .localStateStoreDir(tempDir)
                .build();
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (null != streamName) {
            namespace.deleteLog(streamName);
        }

        if (null != store) {
            store.close();
        }
        if (null != tempDir) {
            FileUtils.deleteDirectory(tempDir);
        }
        super.teardown();
    }

    @Test(expected = NullPointerException.class)
    public void testInitMissingStreamName() throws Exception {
        this.streamName = "test-init-missing-stream-name";
        StateStoreSpec spec = StateStoreSpec.builder()
                .name(streamName)
                .keyCoder(ByteArrayCoder.of())
                .valCoder(ByteArrayCoder.of())
                .localStateStoreDir(tempDir)
                .build();
        result(store.init(spec));
    }

    private byte[] getKey(int i) {
        return String.format("key-%05d", i).getBytes(UTF_8);
    }

    private byte[] getValue(int i) {
        return String.format("value-%05d", i).getBytes(UTF_8);
    }

    @Test
    public void testBasicOps() throws Exception {
        this.streamName = "test-basic-ops";
        StateStoreSpec spec = initSpec(streamName);
        result(store.init(spec));

        // normal put
        {
            assertNull(result(store.get(getKey(0))));
            result(store.put(getKey(0), getValue(0)));
            assertArrayEquals(getValue(0), result(store.get(getKey(0))));
        }

        // putIfAbsent
        {
            // failure case
            assertArrayEquals(getValue(0), result(store.putIfAbsent(getKey(0), getValue(99))));
            assertArrayEquals(getValue(0), result(store.get(getKey(0))));
            // success case
            byte[] key1 = getKey(1);
            assertNull(result(store.putIfAbsent(key1, getValue(1))));
            assertArrayEquals(getValue(1), result(store.get(key1)));
        }

        // delete(k)
        {
            // key not found
            assertNull(result(store.delete(getKey(99))));
            // key exists
            int key = 0;
            assertArrayEquals(getValue(key), result(store.get(getKey(key))));
            assertArrayEquals(getValue(key), result(store.delete(getKey(key))));
            assertNull(result(store.get(getKey(key))));
        }
    }

}

