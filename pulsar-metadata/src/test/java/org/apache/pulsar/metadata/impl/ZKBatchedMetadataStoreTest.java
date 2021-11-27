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

import static org.apache.zookeeper.client.ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.impl.batch.MetadataOp;
import org.assertj.core.util.Lists;
import org.testng.Assert;
import org.testng.annotations.Test;


@Slf4j
public class ZKBatchedMetadataStoreTest extends BaseMetadataStoreTest {

    @Test
    public void testBatchRead() throws Exception {
        ZKBatchedMetadataStore batchedStore = new ZKBatchedMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), false);

        batchedStore.put("/testBatchRead/a", "data1".getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get();
        batchedStore.put("/testBatchRead/b", "data2".getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get();

        MetadataOp.OpGetData getData1 = MetadataOp.getData("/testBatchRead/a");
        MetadataOp.OpGetData getData2 = MetadataOp.getData("/testBatchRead/b");
        MetadataOp.OpGetChildren getChildren = MetadataOp.getChildren("/testBatchRead");

        batchedStore.executeBatchReadOps(Lists.newArrayList(getData1, getData2, getChildren));

        Assert.assertEquals(getData1.getFuture().get().get().getValue(), "data1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(getData2.getFuture().get().get().getValue(), "data2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(getChildren.getFuture().get(), Lists.newArrayList("a", "b"));
    }

    @Test
    public void testFallbackToSingleOps() throws Exception {
        ZKBatchedMetadataStore batchedStore = new ZKBatchedMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), false);

        batchedStore.put("/testFallback/a", "data1".getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get();
        batchedStore.put("/testFallback/b", "data2".getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get();

        MetadataOp.OpGetData getData1 = MetadataOp.getData("/testFallback/a");
        MetadataOp.OpGetData getData2 = MetadataOp.getData("/testFallback/b");
        MetadataOp.OpGetChildren getChildren = MetadataOp.getChildren("/testFallback");

        batchedStore.fallbackToSingleOps(Lists.newArrayList(getData1, getData2, getChildren));

        Assert.assertEquals(getData1.getFuture().get().get().getValue(), "data1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(getData2.getFuture().get().get().getValue(), "data2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(getChildren.getFuture().get(), Lists.list("a", "b"));
    }

    @Test
    public void testAutoFallbackWithResultSizeOutOfRange() throws Exception {
        int dataSize = CLIENT_MAX_PACKET_LENGTH_DEFAULT / 2;
        ZKBatchedMetadataStore batchedStore = new ZKBatchedMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), false);
        byte[] data = new byte[dataSize];
        batchedStore.put("/testAutoFallbackWithResultSizeOutOfRange/a", data, Optional.of(-1L)).get();
        batchedStore.put("/testAutoFallbackWithResultSizeOutOfRange/b", data, Optional.of(-1L)).get();
        MetadataOp.OpGetData getData1 = MetadataOp.getData("/testAutoFallbackWithResultSizeOutOfRange/a");
        MetadataOp.OpGetData getData2 = MetadataOp.getData("/testAutoFallbackWithResultSizeOutOfRange/b");
        //
        batchedStore.executeBatchReadOps(Lists.newArrayList(getData1, getData2));

        Assert.assertEquals(getData1.getFuture().get().get().getValue().length, dataSize);
        Assert.assertEquals(getData2.getFuture().get().get().getValue().length, dataSize);
    }
}
