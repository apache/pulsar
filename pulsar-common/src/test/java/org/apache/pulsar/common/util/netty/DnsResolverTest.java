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

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.channel.EventLoop;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DnsResolverTest {

    @Test
    public void testMaxTtl() throws Exception {
        DnsNameResolverBuilder dnsNameResolverBuilder = new DnsNameResolverBuilder(Mockito.mock(EventLoop.class));
        assertThat(dnsNameResolverBuilder).isNotNull();
        DnsResolverUtil.applyJdkDnsCacheSettings(dnsNameResolverBuilder);

        CountDownLatch latch = new CountDownLatch(1);
        try (MockedConstruction<?> ignore = Mockito.mockConstruction(
                DnsNameResolver.class, (a, b) -> latch.countDown())) {
            dnsNameResolverBuilder.build();
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
