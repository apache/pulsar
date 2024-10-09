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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.testng.annotations.Test;

/**
 * Unit test for {@link MessageIdAdvUtils}.
 */
public class MessageIdAdvUtilsTest {

    /**
     * Call <code>acknowledge</code> concurrently with batch message, and verify that only return true once
     *
     * @see MessageIdAdvUtils#acknowledge(MessageIdAdv, boolean)
     * @see MessageIdAdv#getAckSet()
     */
    @Test
    public void testAcknowledgeIndividualConcurrently() throws InterruptedException {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("pulsar-consumer-%d").build();
        @Cleanup("shutdown")
        ExecutorService executorService = Executors.newCachedThreadPool(threadFactory);
        for (int i = 0; i < 100; i++) {
            int batchSize = 32;
            BitSet bitSet = new BitSet(batchSize);
            bitSet.set(0, batchSize);
            AtomicInteger individualAcked = new AtomicInteger();
            Phaser phaser = new Phaser(1);
            CountDownLatch finishLatch = new CountDownLatch(batchSize);
            for (int batchIndex = 0; batchIndex < batchSize; batchIndex++) {
                phaser.register();
                BatchMessageIdImpl messageId = new BatchMessageIdImpl(1, 0, 0, batchIndex, batchSize, bitSet);
                executorService.execute(() -> {
                    try {
                        phaser.arriveAndAwaitAdvance();
                        if (MessageIdAdvUtils.acknowledge(messageId, true)) {
                            individualAcked.incrementAndGet();
                        }
                    } finally {
                        finishLatch.countDown();
                    }
                });
            }
            phaser.arriveAndDeregister();
            finishLatch.await();
            assertEquals(individualAcked.get(), 1);
        }
    }
}
