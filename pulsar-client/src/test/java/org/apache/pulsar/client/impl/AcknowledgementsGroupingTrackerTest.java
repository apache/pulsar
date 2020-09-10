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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AcknowledgementsGroupingTrackerTest {

    private ClientCnx cnx;
    private ConsumerImpl<?> consumer;
    private EventLoopGroup eventLoopGroup;

    @BeforeClass
    public void setup() {
        eventLoopGroup = new NioEventLoopGroup(1);
        consumer = mock(ConsumerImpl.class);
        consumer.unAckedChunckedMessageIdSequenceMap = new ConcurrentOpenHashMap<>();
        cnx = mock(ClientCnx.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(cnx.ctx()).thenReturn(ctx);
    }

    @AfterClass
    public void teardown() {
        eventLoopGroup.shutdownGracefully();
    }

    @Test
    public void testAckTracker() throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.SECONDS.toMicros(10));
        PersistentAcknowledgmentsGroupingTracker tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);
        MessageIdImpl msg3 = new MessageIdImpl(5, 3, 0);
        MessageIdImpl msg4 = new MessageIdImpl(5, 4, 0);
        MessageIdImpl msg5 = new MessageIdImpl(5, 5, 0);
        MessageIdImpl msg6 = new MessageIdImpl(5, 6, 0);

        assertFalse(tracker.isDuplicate(msg1));

        tracker.addAcknowledgment(msg1, AckType.Individual, Collections.emptyMap(), -1, -1);
        assertTrue(tracker.isDuplicate(msg1));

        assertFalse(tracker.isDuplicate(msg2));

        tracker.addAcknowledgment(msg5, AckType.Cumulative, Collections.emptyMap(), -1, -1);
        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        // Flush while disconnected. the internal tracking will not change
        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.addAcknowledgment(msg6, AckType.Individual, Collections.emptyMap(), -1, -1);
        assertTrue(tracker.isDuplicate(msg6));

        when(consumer.getClientCnx()).thenReturn(cnx);

        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.close();
    }

    @Test
    public void testBatchAckTracker() throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.SECONDS.toMicros(10));
        PersistentAcknowledgmentsGroupingTracker tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);
        MessageIdImpl msg3 = new MessageIdImpl(5, 3, 0);
        MessageIdImpl msg4 = new MessageIdImpl(5, 4, 0);
        MessageIdImpl msg5 = new MessageIdImpl(5, 5, 0);
        MessageIdImpl msg6 = new MessageIdImpl(5, 6, 0);

        assertFalse(tracker.isDuplicate(msg1));

        tracker.addListAcknowledgment(Collections.singletonList(msg1), AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));

        assertFalse(tracker.isDuplicate(msg2));

        tracker.addListAcknowledgment(Collections.singletonList(msg5), AckType.Cumulative, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        // Flush while disconnected. the internal tracking will not change
        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.addListAcknowledgment(Collections.singletonList(msg6), AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg6));

        when(consumer.getClientCnx()).thenReturn(cnx);

        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.close();
    }

    @Test
    public void testImmediateAckingTracker() throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(0);
        PersistentAcknowledgmentsGroupingTracker tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);

        assertFalse(tracker.isDuplicate(msg1));

        when(consumer.getClientCnx()).thenReturn(null);

        tracker.addAcknowledgment(msg1, AckType.Individual, Collections.emptyMap(), -1, -1);
        assertFalse(tracker.isDuplicate(msg1));

        when(consumer.getClientCnx()).thenReturn(cnx);

        tracker.flush();
        assertFalse(tracker.isDuplicate(msg1));

        tracker.addAcknowledgment(msg2, AckType.Individual, Collections.emptyMap(), -1, -1);
        // Since we were connected, the ack went out immediately
        assertFalse(tracker.isDuplicate(msg2));
        tracker.close();
    }

    @Test
    public void testImmediateBatchAckingTracker() throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(0);
        PersistentAcknowledgmentsGroupingTracker tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);

        assertFalse(tracker.isDuplicate(msg1));

        when(consumer.getClientCnx()).thenReturn(null);

        tracker.addListAcknowledgment(Collections.singletonList(msg1), AckType.Individual, Collections.emptyMap());
        tracker.flush();
        //cnx is null can not flush
        assertTrue(tracker.isDuplicate(msg1));

        when(consumer.getClientCnx()).thenReturn(cnx);

        tracker.flush();
        assertFalse(tracker.isDuplicate(msg1));

        tracker.addListAcknowledgment(Collections.singletonList(msg2), AckType.Individual, Collections.emptyMap());
        // Since we were connected, the ack went out immediately
        assertFalse(tracker.isDuplicate(msg2));
        tracker.close();
    }

    @Test
    public void testAckTrackerMultiAck() throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.SECONDS.toMicros(10));
        PersistentAcknowledgmentsGroupingTracker tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12_VALUE);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);
        MessageIdImpl msg3 = new MessageIdImpl(5, 3, 0);
        MessageIdImpl msg4 = new MessageIdImpl(5, 4, 0);
        MessageIdImpl msg5 = new MessageIdImpl(5, 5, 0);
        MessageIdImpl msg6 = new MessageIdImpl(5, 6, 0);

        assertFalse(tracker.isDuplicate(msg1));

        tracker.addAcknowledgment(msg1, AckType.Individual, Collections.emptyMap(), -1, -1);
        assertTrue(tracker.isDuplicate(msg1));

        assertFalse(tracker.isDuplicate(msg2));

        tracker.addAcknowledgment(msg5, AckType.Cumulative, Collections.emptyMap(), -1, -1);
        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        // Flush while disconnected. the internal tracking will not change
        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.addAcknowledgment(msg6, AckType.Individual, Collections.emptyMap(), -1, -1);
        assertTrue(tracker.isDuplicate(msg6));

        when(consumer.getClientCnx()).thenReturn(cnx);

        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.close();
    }

    @Test
    public void testBatchAckTrackerMultiAck() throws Exception {
        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.SECONDS.toMicros(10));
        PersistentAcknowledgmentsGroupingTracker tracker = new PersistentAcknowledgmentsGroupingTracker(consumer, conf, eventLoopGroup);

        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12_VALUE);

        MessageIdImpl msg1 = new MessageIdImpl(5, 1, 0);
        MessageIdImpl msg2 = new MessageIdImpl(5, 2, 0);
        MessageIdImpl msg3 = new MessageIdImpl(5, 3, 0);
        MessageIdImpl msg4 = new MessageIdImpl(5, 4, 0);
        MessageIdImpl msg5 = new MessageIdImpl(5, 5, 0);
        MessageIdImpl msg6 = new MessageIdImpl(5, 6, 0);

        assertFalse(tracker.isDuplicate(msg1));

        tracker.addListAcknowledgment(Collections.singletonList(msg1), AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));

        assertFalse(tracker.isDuplicate(msg2));

        tracker.addListAcknowledgment(Collections.singletonList(msg5), AckType.Cumulative, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        // Flush while disconnected. the internal tracking will not change
        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.addListAcknowledgment(Collections.singletonList(msg6), AckType.Individual, Collections.emptyMap());
        assertTrue(tracker.isDuplicate(msg6));

        when(consumer.getClientCnx()).thenReturn(cnx);

        tracker.flush();

        assertTrue(tracker.isDuplicate(msg1));
        assertTrue(tracker.isDuplicate(msg2));
        assertTrue(tracker.isDuplicate(msg3));

        assertTrue(tracker.isDuplicate(msg4));
        assertTrue(tracker.isDuplicate(msg5));
        assertFalse(tracker.isDuplicate(msg6));

        tracker.close();
    }
}
