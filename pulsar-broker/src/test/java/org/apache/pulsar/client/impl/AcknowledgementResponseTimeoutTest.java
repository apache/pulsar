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
package org.apache.pulsar.client.impl;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class AcknowledgementResponseTimeoutTest {

    private ConsumerImpl<?> consumer;
    private EventLoopGroup eventLoopGroup;
    private AtomicLong atomicLong = new AtomicLong(0);
    private final long TIME_OUT = 3000L;

    @BeforeClass
    public void setup() throws Exception {
        eventLoopGroup = new NioEventLoopGroup(2);
        consumer = mock(ConsumerImpl.class);

        Field field = ConsumerBase.class.getDeclaredField("unAckedChunkedMessageIdSequenceMap");
        field.setAccessible(true);
        field.set(consumer, new ConcurrentOpenHashMap<>());

        ClientCnx cnx = mock(ClientCnx.class);
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        Timer timer = new HashedWheelTimer();
        doReturn(cnx).when(consumer).getClientCnx();
        doReturn(client).when(consumer).getClient();
        doReturn(timer).when(client).timer();
        doReturn(atomicLong.getAndIncrement()).when(client).newRequestId();

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelPromise promise = mock(ChannelPromise.class);
        doReturn(ctx).when(cnx).ctx();
        doReturn(promise).when(ctx).voidPromise();
    }

    @DataProvider(name = "acknowledgementsGroupTimeMicros")
    public Object[][] acknowledgementsGroupTimeMicros() {
        return new Object[][] { { 0, AckType.Cumulative }, { 100, AckType.Cumulative },
                { 0, AckType.Individual }, { 100, AckType.Individual } };
    }

    @AfterClass
    public void teardown() {
        eventLoopGroup.shutdownGracefully();
    }

    @Test(dataProvider = "acknowledgementsGroupTimeMicros")
    public void testSingleMessageAck(long acknowledgementsGroupTimeMicros, AckType ackType) {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(acknowledgementsGroupTimeMicros);
        conf.setAckResponseTimeout(TIME_OUT);
        AcknowledgmentsGroupingTracker tracker =
                new PersistentAcknowledgmentsWithResponseGroupingTracker(consumer, conf, eventLoopGroup);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        CompletableFuture<Void> completableFuture =
                tracker.addAcknowledgment(msg1, ackType, Collections.emptyMap(), null);
        Awaitility.await().atLeast(TIME_OUT, TimeUnit.MILLISECONDS).until(() -> {
            try {
                completableFuture.get();
                return false;
            } catch (Exception e) {
                return e.getCause() instanceof PulsarClientException.AckResponseTimeoutException;
            }
        });
    }

    @Test(dataProvider = "acknowledgementsGroupTimeMicros")
    public void testMessagesAck(long acknowledgementsGroupTimeMicros, AckType ackType) {
        final int listSize = 5;
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(acknowledgementsGroupTimeMicros);
        conf.setAckResponseTimeout(TIME_OUT);
        AcknowledgmentsGroupingTracker tracker =
                new PersistentAcknowledgmentsWithResponseGroupingTracker(consumer, conf, eventLoopGroup);
        List<MessageIdImpl> messageIds = new ArrayList<>();
        for (int i = 0; i < listSize; i++ ) {
            messageIds.add(new MessageIdImpl(5, i, 0));
        }
        CompletableFuture<Void> completableFuture =
                tracker.addListAcknowledgment(messageIds, ackType, Collections.emptyMap());
        Awaitility.await().atLeast(TIME_OUT, TimeUnit.MILLISECONDS).until(() -> {
            try {
                completableFuture.get();
                return false;
            } catch (Exception e) {
                return e.getCause() instanceof PulsarClientException.AckResponseTimeoutException;
            }
        });
    }

    @Test(dataProvider = "acknowledgementsGroupTimeMicros")
    public void testBatchIndexAck(long acknowledgementsGroupTimeMicros, AckType ackType) {
        final int listSize = 5;
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(acknowledgementsGroupTimeMicros);
        conf.setAckResponseTimeout(TIME_OUT);
        AcknowledgmentsGroupingTracker tracker =
                new PersistentAcknowledgmentsWithResponseGroupingTracker(consumer, conf, eventLoopGroup);

        BatchMessageAcker acker = BatchMessageAcker.newAcker(5);
        for (int i = 0; i < listSize; i++ ) {
            BatchMessageIdImpl batchMessageId =
                    new BatchMessageIdImpl(listSize, listSize, 0, i, listSize, acker);
            CompletableFuture<Void> completableFuture =
                    tracker.addBatchIndexAcknowledgment(batchMessageId, i, listSize,
                            ackType, Collections.emptyMap(), null);
            Awaitility.await().atLeast(TIME_OUT, TimeUnit.MILLISECONDS)
                    .atMost(4000L, TimeUnit.MILLISECONDS).until(() -> {
                try {
                    completableFuture.get();
                    return false;
                } catch (Exception e) {
                    return e.getCause() instanceof PulsarClientException.AckResponseTimeoutException;
                }
            });
        }
    }
}
