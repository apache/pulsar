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
package org.apache.pulsar.functions.instance.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.testng.annotations.Test;

public class LazyPulsarClientV5Test {

    @Test
    public void testCreatesTheClientOnceOnFirstUse() throws Exception {
        PulsarClientBuilder builder = mock(PulsarClientBuilder.class);
        PulsarClient client = mock(PulsarClient.class);
        when(builder.build()).thenReturn(client);
        AtomicInteger builders = new AtomicInteger();
        LazyPulsarClientV5 lazyClient = new LazyPulsarClientV5(() -> {
            builders.incrementAndGet();
            return builder;
        });

        assertThat(builders).hasValue(0);
        assertThat(lazyClient.get()).isSameAs(client);
        assertThat(lazyClient.get()).isSameAs(client);
        assertThat(builders).hasValue(1);

        lazyClient.close();
        verify(client, times(1)).close();
        assertThatThrownBy(lazyClient::get).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testCloseWithoutClient() {
        AtomicInteger builders = new AtomicInteger();
        LazyPulsarClientV5 lazyClient = new LazyPulsarClientV5(() -> {
            builders.incrementAndGet();
            return mock(PulsarClientBuilder.class);
        });
        lazyClient.close();
        assertThat(builders).hasValue(0);
    }
}
