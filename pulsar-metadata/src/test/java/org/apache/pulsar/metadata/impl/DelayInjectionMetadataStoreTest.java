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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DelayInjectionMetadataStoreTest {
    @Test
    public void testRandomDelay() throws Exception {
        MetadataStoreExtended innerStore =
                MetadataStoreFactoryImpl.createExtended("memory://local", MetadataStoreConfig.builder().build());
        DelayInjectionMetadataStore store = new DelayInjectionMetadataStore(innerStore);

        store.setMaxRandomDelayMills(200);
        store.setMinRandomDelayMills(100);
        byte[] data = new byte[]{1, 2, 3};
        long start = System.currentTimeMillis();
        store.put("/data", data, Optional.empty()).get();
        Assert.assertTrue(System.currentTimeMillis() - start >= 100);
        start = System.currentTimeMillis();
        Assert.assertEquals(store.get("/data").get().get().getValue(), data);
        Assert.assertTrue(System.currentTimeMillis() - start >= 100);
    }

    @Test
    public void testConditionDelay() throws Exception {
        MetadataStoreExtended innerStore =
                MetadataStoreFactoryImpl.createExtended("memory://local", MetadataStoreConfig.builder().build());
        DelayInjectionMetadataStore store = new DelayInjectionMetadataStore(innerStore);

        CompletableFuture<Void> delay = new CompletableFuture<>();
        store.delayConditional(delay, (operationType, path) -> {
            return operationType == DelayInjectionMetadataStore.OperationType.GET && path.equals("/data");
        });

        byte[] data = new byte[]{1, 2, 3};
        store.put("/data", data, Optional.empty()).get(); //NO delay.

        long start = System.currentTimeMillis();
        CompletableFuture<Optional<GetResult>> getFuture = store.get("/data");
        Thread.sleep(1000);
        delay.complete(null); //delay finish.
        Assert.assertEquals(getFuture.get().get().getValue(), data);
        Assert.assertTrue(System.currentTimeMillis() - start >= 1000);

        //delay only applied once.
        Assert.assertEquals(store.get("/data").get().get().getValue(), data);
    }
}