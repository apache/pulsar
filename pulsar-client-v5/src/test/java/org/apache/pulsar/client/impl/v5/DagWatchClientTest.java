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
package org.apache.pulsar.client.impl.v5;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

public class DagWatchClientTest {

    private static final String TOPIC = "topic://public/default/dag-watch-test";

    /** A watch that has received its initial layout, so a later disconnect is a reconnect case. */
    private static DagWatchClient watchWithLayout(PulsarClientImpl v4Client) {
        DagWatchClient watch = new DagWatchClient(v4Client, TopicName.get(TOPIC));
        watch.onUpdate(new ScalableTopicDAG().setEpoch(1L), TOPIC);
        return watch;
    }

    @Test
    public void testDisconnectSchedulesReconnectWhileClientOpen() {
        PulsarClientImpl v4Client = mock(PulsarClientImpl.class);
        Timer timer = mock(Timer.class);
        when(v4Client.timer()).thenReturn(timer);
        DagWatchClient watch = watchWithLayout(v4Client);

        watch.connectionClosed();

        verify(timer).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testDisconnectDoesNotReconnectWhileClientClosing() {
        PulsarClientImpl v4Client = mock(PulsarClientImpl.class);
        when(v4Client.isClosed()).thenReturn(true);
        DagWatchClient watch = watchWithLayout(v4Client);

        watch.connectionClosed();

        verify(v4Client, never()).timer();
    }
}
