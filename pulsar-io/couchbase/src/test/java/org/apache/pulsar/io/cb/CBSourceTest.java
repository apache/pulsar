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
package org.apache.pulsar.io.cb;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit test for cb source.
 */
@Slf4j
public class CBSourceTest
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void basicTest() throws  Exception {
        log.info("mainTest starting...");

        final Client client = Client.configure()
                .hostnames("localhost")
                .bucket("travel-sample")
                .build();

        final AtomicLong totalSize = new AtomicLong();
        final AtomicLong docCount = new AtomicLong();

        client.dataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ChannelFlowController channelFlowController, ByteBuf byteBuf) {
                if (DcpMutationMessage.is(byteBuf)) {
                    docCount.incrementAndGet();
                    totalSize.addAndGet(DcpMutationMessage.content(byteBuf).readableBytes());
                }

                byteBuf.release();
            }
        });

        client.connect().await();

        client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).await();

        client.startStreaming().await();

        while (true) {
            if (client.sessionState().isAtEnd()) {
                break;
            }

            Thread.sleep(500);
        }

        log.info("Total Docs : {}", docCount.get());
        log.info("Total Bytes: {}", totalSize.get());

        client.disconnect().await();

        log.info("mainTest completed.");
    }
}
