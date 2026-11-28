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
package org.apache.pulsar.compaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.common.util.FutureUtil;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-compaction")
public class CompactorReaderFailureTest {

    private ScheduledExecutorService scheduler;
    private Compactor compactor;

    @BeforeMethod
    public void setup() {
        scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compactor-test").setDaemon(true).build());
        compactor = new PublishingOrderCompactor(new ServiceConfiguration(), mock(PulsarClient.class),
                mock(BookKeeper.class), scheduler);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        scheduler.shutdownNow();
    }

    @Test
    public void testCompactionFailedCountWhenReaderCreationFails() throws Exception {
        String topic = "persistent://my-property/my-ns/my-topic";
        try (MockedStatic<RawReader> rawReaderMock = mockStatic(RawReader.class)) {
            rawReaderMock.when(() -> RawReader.create(any(PulsarClient.class), anyString(), anyString(),
                            eq(false), eq(false)))
                    .thenReturn(FutureUtil.failedFuture(
                            new PulsarClientException("mocked reader creation failure")));
            try {
                compactor.compact(topic).get();
                fail("compact() should have failed when the raw reader cannot be created");
            } catch (ExecutionException e) {
                // expected: the mocked RawReader.create() failure must propagate
            }
        }
        CompactionRecord record = compactor.getStats().getCompactionRecordForTopic(topic)
                .orElseThrow(() -> new AssertionError("No compaction record for topic " + topic));
        assertEquals(record.getCompactionFailedCount(), 1L,
                "A failed raw reader creation must increment the compaction failure counter");
        assertEquals(record.getCompactionSucceedCount(), 0L);
        assertTrue(record.getLastCompactionFailedTimestamp() > 0L);
    }
}
