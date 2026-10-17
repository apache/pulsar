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
package org.apache.pulsar.common.util.netty;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

/**
 * Tests the auto-read behavior of {@link PulsarFlowControlHandler} that Pulsar's reactive request throttling
 * depends on: a downstream handler disabling auto-read from inside {@code channelRead} must stop the delivery
 * of queued messages immediately. Netty's {@code FlowControlHandler} lost this behavior in Netty 4.2.15
 * (netty/netty#16837), which is why Pulsar carries this copy of the previous implementation.
 */
public class PulsarFlowControlHandlerTest {

    /**
     * Downstream handler that records received messages and disables auto-read whenever the number of received
     * messages reaches the next configured threshold, mimicking how ServerCnxThrottleTracker pauses a connection
     * while processing a message.
     */
    private static class ThrottlingHandler extends ChannelInboundHandlerAdapter {
        private final List<Object> received = new ArrayList<>();
        private int pauseAtCount;

        ThrottlingHandler(int pauseAtCount) {
            this.pauseAtCount = pauseAtCount;
        }

        void pauseAgainAtCount(int count) {
            this.pauseAtCount = count;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            received.add(msg);
            if (received.size() == pauseAtCount) {
                ctx.channel().config().setAutoRead(false);
            }
        }
    }

    @Test
    public void shouldStopDeliveryWhenAutoReadIsDisabledDuringChannelRead() {
        PulsarFlowControlHandler flowControlHandler = new PulsarFlowControlHandler();
        ThrottlingHandler throttlingHandler = new ThrottlingHandler(1);
        EmbeddedChannel channel = new EmbeddedChannel(flowControlHandler, throttlingHandler);

        channel.writeInbound("1", "2", "3", "4", "5");

        // the downstream handler disabled auto-read while processing the first message; the remaining messages
        // must be held in the flow control handler's queue instead of being delivered
        assertEquals(throttlingHandler.received.size(), 1);
        assertFalse(flowControlHandler.isQueueEmpty());

        // re-enabling auto-read resumes delivery; the downstream handler pauses again on the next message,
        // so exactly one more message must be delivered (per-message auto-read granularity)
        throttlingHandler.pauseAgainAtCount(2);
        channel.config().setAutoRead(true);
        assertEquals(throttlingHandler.received.size(), 2);
        assertFalse(flowControlHandler.isQueueEmpty());

        // with auto-read left enabled, the rest of the queue drains
        throttlingHandler.pauseAgainAtCount(-1);
        channel.config().setAutoRead(true);
        assertEquals(throttlingHandler.received.size(), 5);
        assertTrue(flowControlHandler.isQueueEmpty());

        assertFalse(channel.finish());
    }

    @Test
    public void shouldDeliverOneMessagePerReadWhenAutoReadIsDisabled() {
        PulsarFlowControlHandler flowControlHandler = new PulsarFlowControlHandler();
        ThrottlingHandler throttlingHandler = new ThrottlingHandler(0);
        EmbeddedChannel channel = new EmbeddedChannel(flowControlHandler, throttlingHandler);
        channel.config().setAutoRead(false);

        // channel activation (with auto-read initially enabled) issued one read() through the pipeline, so there
        // is one outstanding message of read demand that the first written message satisfies
        channel.writeInbound("1", "2", "3");
        assertEquals(throttlingHandler.received.size(), 1);

        channel.read();
        assertEquals(throttlingHandler.received.size(), 2);

        channel.read();
        assertEquals(throttlingHandler.received.size(), 3);
        assertTrue(flowControlHandler.isQueueEmpty());

        assertFalse(channel.finish());
    }
}
