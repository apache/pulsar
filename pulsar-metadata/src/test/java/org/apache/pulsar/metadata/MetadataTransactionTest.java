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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.impl.batching.MetadataOp;
import org.apache.pulsar.metadata.impl.batching.OpGet;
import org.apache.pulsar.metadata.impl.batching.OpPut;
import org.apache.pulsar.metadata.impl.batching.transaction.TransactionMetadataStore;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class MetadataTransactionTest extends BaseMetadataStoreTest {


    @Test(dataProvider = "txImpl")
    public void testTransaction_WorkNormally(String provider, Supplier<String> supplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(supplier.get(), MetadataStoreConfig.builder().build());

        List<MetadataOp> ops = new ArrayList<>();

        // batch put
        {
            OpPut opPut1 = new OpPut("/test1", "value1".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut2 = new OpPut("/test2", "value2".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut3 = new OpPut("/test3", "value3".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut4 = new OpPut("/test4", "value4".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut5 = new OpPut("/test5", "value5".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                    EnumSet.noneOf(CreateOption.class));

            ops.add(opPut1);
            ops.add(opPut2);
            ops.add(opPut3);
            ops.add(opPut4);
            ops.add(opPut5);

            ((TransactionMetadataStore) store).txOperation(ops);

            CountDownLatch latch = new CountDownLatch(1);

            AtomicReference<Throwable> ex = new AtomicReference<>();

            FutureUtil.waitForAll(ops.stream().map(MetadataOp::getFuture).collect(Collectors.toList()))
                    .whenComplete((res, e) -> {
                        latch.countDown();
                        if (e != null) {
                            ex.set(e);
                        }
                    });
            latch.await();
            assertNull(ex.get());
        }

        // batch get
        {
            OpGet opGet1 = new OpGet("/test1");
            OpGet opGet2 = new OpGet("/test2");
            OpGet opGet3 = new OpGet("/test3");
            OpGet opGet4 = new OpGet("/test4");
            OpGet opGet5 = new OpGet("/test5");

            ops.clear();
            ops.add(opGet1);
            ops.add(opGet2);
            ops.add(opGet3);
            ops.add(opGet4);
            ops.add(opGet5);

            ((TransactionMetadataStore) store).txOperation(ops);

            assertEquals(new String(opGet1.getFuture().get().get().getValue()), "value1");
            assertEquals(new String(opGet2.getFuture().get().get().getValue()), "value2");
            assertEquals(new String(opGet3.getFuture().get().get().getValue()), "value3");
            assertEquals(new String(opGet4.getFuture().get().get().getValue()), "value4");
            assertEquals(new String(opGet5.getFuture().get().get().getValue()), "value5");
        }


        // batch put with bad version exception
        {
            OpPut opPut1 = new OpPut("/test1", "value6".getBytes(StandardCharsets.UTF_8), Optional.of(0L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut2 = new OpPut("/test2", "value7".getBytes(StandardCharsets.UTF_8), Optional.of(0L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut3 = new OpPut("/test3", "value8".getBytes(StandardCharsets.UTF_8), Optional.of(0L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut4 = new OpPut("/test4", "value9".getBytes(StandardCharsets.UTF_8), Optional.of(0L),
                    EnumSet.noneOf(CreateOption.class));

            //bad version opPut, it will roll back op1-op4
            OpPut opPut5 = new OpPut("/test5", "value10".getBytes(StandardCharsets.UTF_8), Optional.of(0L),
                    EnumSet.noneOf(CreateOption.class));

            ops.clear();
            ops.add(opPut1);
            ops.add(opPut2);
            ops.add(opPut3);
            ops.add(opPut4);
            ops.add(opPut5);

            ((TransactionMetadataStore) store).txOperation(ops);


            CountDownLatch latch = new CountDownLatch(1);

            AtomicReference<Throwable> ex = new AtomicReference<>();

            FutureUtil.waitForAll(ops.stream().map(MetadataOp::getFuture).collect(Collectors.toList()))
                    .whenComplete((res, e) -> {
                        latch.countDown();
                        if (e != null) {
                            ex.set(e);
                        }
                    });
            latch.await();
            assertNull(ex.get());
        }

        //batch get, the data didn't change
        {
            OpGet opGet1 = new OpGet("/test1");
            OpGet opGet2 = new OpGet("/test2");
            OpGet opGet3 = new OpGet("/test3");
            OpGet opGet4 = new OpGet("/test4");
            OpGet opGet5 = new OpGet("/test5");

            ops.clear();
            ops.add(opGet1);
            ops.add(opGet2);
            ops.add(opGet3);
            ops.add(opGet4);
            ops.add(opGet5);

            ((TransactionMetadataStore) store).txOperation(ops);

            assertEquals(new String(opGet1.getFuture().get().get().getValue()), "value6");
            assertEquals(new String(opGet2.getFuture().get().get().getValue()), "value7");
            assertEquals(new String(opGet3.getFuture().get().get().getValue()), "value8");
            assertEquals(new String(opGet4.getFuture().get().get().getValue()), "value9");
            assertEquals(new String(opGet5.getFuture().get().get().getValue()), "value10");

            CountDownLatch latch = new CountDownLatch(1);

            AtomicReference<Throwable> ex = new AtomicReference<>();

            FutureUtil.waitForAll(ops.stream().map(MetadataOp::getFuture).collect(Collectors.toList()))
                    .whenComplete((res, e) -> {
                        latch.countDown();
                        if (e != null) {
                            ex.set(e);
                        }
                    });
            latch.await();
            assertNull(ex.get());
        }
    }


    @Test(dataProvider = "txImpl")
    public void testTransaction_RollBack(String provider, Supplier<String> supplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(supplier.get(), MetadataStoreConfig.builder().build());

        List<MetadataOp> ops = new ArrayList<>();

        // batch put
        {
            OpPut opPut1 = new OpPut("/test1", "value1".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut2 = new OpPut("/test2", "value2".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut3 = new OpPut("/test3", "value3".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut4 = new OpPut("/test4", "value4".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut5 = new OpPut("/test5", "value5".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                    EnumSet.noneOf(CreateOption.class));

            ops.add(opPut1);
            ops.add(opPut2);
            ops.add(opPut3);
            ops.add(opPut4);
            ops.add(opPut5);

            ((TransactionMetadataStore) store).txOperation(ops);

            CountDownLatch latch = new CountDownLatch(1);

            AtomicReference<Throwable> ex = new AtomicReference<>();

            FutureUtil.waitForAll(ops.stream().map(MetadataOp::getFuture).collect(Collectors.toList()))
                    .whenComplete((res, e) -> {
                        latch.countDown();
                        if (e != null) {
                            ex.set(e);
                        }
                    });
            latch.await();
            assertNull(ex.get());
        }

        // batch get
        {
            OpGet opGet1 = new OpGet("/test1");
            OpGet opGet2 = new OpGet("/test2");
            OpGet opGet3 = new OpGet("/test3");
            OpGet opGet4 = new OpGet("/test4");
            OpGet opGet5 = new OpGet("/test5");

            ops.clear();
            ops.add(opGet1);
            ops.add(opGet2);
            ops.add(opGet3);
            ops.add(opGet4);
            ops.add(opGet5);

            ((TransactionMetadataStore) store).txOperation(ops);

            assertEquals(new String(opGet1.getFuture().get().get().getValue()), "value1");
            assertEquals(new String(opGet2.getFuture().get().get().getValue()), "value2");
            assertEquals(new String(opGet3.getFuture().get().get().getValue()), "value3");
            assertEquals(new String(opGet4.getFuture().get().get().getValue()), "value4");
            assertEquals(new String(opGet5.getFuture().get().get().getValue()), "value5");
        }


        // batch put with bad version exception
        {
            OpPut opPut1 = new OpPut("/test1", "value6".getBytes(StandardCharsets.UTF_8), Optional.of(0L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut2 = new OpPut("/test2", "value7".getBytes(StandardCharsets.UTF_8), Optional.of(0L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut3 = new OpPut("/test3", "value8".getBytes(StandardCharsets.UTF_8), Optional.of(0L),
                    EnumSet.noneOf(CreateOption.class));
            OpPut opPut4 = new OpPut("/test4", "value9".getBytes(StandardCharsets.UTF_8), Optional.of(0L),
                    EnumSet.noneOf(CreateOption.class));

            //bad version opPut, it will roll back op1-op4
            OpPut opPut5 = new OpPut("/test5", "value10".getBytes(StandardCharsets.UTF_8), Optional.of(1L),
                    EnumSet.noneOf(CreateOption.class));

            ops.clear();
            ops.add(opPut1);
            ops.add(opPut2);
            ops.add(opPut3);
            ops.add(opPut4);
            ops.add(opPut5);

            ((TransactionMetadataStore) store).txOperation(ops);


            CountDownLatch latch = new CountDownLatch(1);

            AtomicReference<Throwable> ex = new AtomicReference<>();

            FutureUtil.waitForAll(ops.stream().map(MetadataOp::getFuture).collect(Collectors.toList()))
                    .whenComplete((res, e) -> {
                        latch.countDown();
                        if (e != null) {
                            ex.set(e);
                        }
                    });
            latch.await();
            assertNotNull(ex.get());
            Assert.assertEquals(MetadataStoreException.TransactionFailedException.class, ex.get().getCause().getClass());
        }

        //batch get, the data didn't change
        {
            OpGet opGet1 = new OpGet("/test1");
            OpGet opGet2 = new OpGet("/test2");
            OpGet opGet3 = new OpGet("/test3");
            OpGet opGet4 = new OpGet("/test4");
            OpGet opGet5 = new OpGet("/test5");

            ops.clear();
            ops.add(opGet1);
            ops.add(opGet2);
            ops.add(opGet3);
            ops.add(opGet4);
            ops.add(opGet5);

            ((TransactionMetadataStore) store).txOperation(ops);

            assertEquals(new String(opGet1.getFuture().get().get().getValue()), "value1");
            assertEquals(new String(opGet2.getFuture().get().get().getValue()), "value2");
            assertEquals(new String(opGet3.getFuture().get().get().getValue()), "value3");
            assertEquals(new String(opGet4.getFuture().get().get().getValue()), "value4");
            assertEquals(new String(opGet5.getFuture().get().get().getValue()), "value5");

            CountDownLatch latch = new CountDownLatch(1);

            AtomicReference<Throwable> ex = new AtomicReference<>();

            FutureUtil.waitForAll(ops.stream().map(MetadataOp::getFuture).collect(Collectors.toList()))
                    .whenComplete((res, e) -> {
                        latch.countDown();
                        if (e != null) {
                            ex.set(e);
                        }
                    });
            latch.await();
            assertNull(ex.get());
        }
    }
}
