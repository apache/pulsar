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
package org.apache.pulsar.client.impl.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

public class WatchTcAssignmentsDiscoveryTest {

    @Test
    public void testUnsupportedBrokerFailsWithoutRetrying() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ClientCnx cnx = mock(ClientCnx.class);
        when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
        when(client.getAnyBrokerProxyConnection()).thenReturn(CompletableFuture.completedFuture(cnx));
        WatchTcAssignmentsDiscovery discovery = new WatchTcAssignmentsDiscovery(client);
        try {
            CompletableFuture<Void> start = discovery.start();
            assertThat(start).isCompletedExceptionally();
            assertThatThrownBy(start::join)
                    .hasCauseInstanceOf(PulsarClientException.NotSupportedException.class)
                    .hasMessageContaining("Broker does not support scalable-topics transactions");
            verify(client, never()).timer();
            verify(cnx, never()).registerTcAssignmentsWatcher(anyLong(), any());
            verify(cnx, never()).ctx();
        } finally {
            discovery.close();
        }
    }
}
