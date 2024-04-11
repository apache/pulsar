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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import io.netty.channel.EventLoop;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import java.security.Security;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DnsResolverTest {
    private static final int MIN_TTL = 0;
    private static final int TTL = 101;
    private static final int NEGATIVE_TTL = 121;
    private static final String CACHE_POLICY_PROP = "networkaddress.cache.ttl";
    private static final String NEGATIVE_CACHE_POLICY_PROP = "networkaddress.cache.negative.ttl";

    private String originalCachePolicy;
    private String originalNegativeCachePolicy;

    @BeforeClass(alwaysRun = true)
    public void beforeClass() {
        originalCachePolicy = Security.getProperty(CACHE_POLICY_PROP);
        originalNegativeCachePolicy = Security.getProperty(NEGATIVE_CACHE_POLICY_PROP);
        Security.setProperty(CACHE_POLICY_PROP, Integer.toString(TTL));
        Security.setProperty(NEGATIVE_CACHE_POLICY_PROP, Integer.toString(NEGATIVE_TTL));
    }

    @AfterClass(alwaysRun = true)
    public void afterClass() {
        Security.setProperty(CACHE_POLICY_PROP, originalCachePolicy != null ? originalCachePolicy : "-1");
        Security.setProperty(NEGATIVE_CACHE_POLICY_PROP,
                originalNegativeCachePolicy != null ? originalNegativeCachePolicy : "0");
    }

    @Test
    public void testTTl() {
        final DnsNameResolverBuilder builder = mock(DnsNameResolverBuilder.class);
        ArgumentCaptor<Integer> minTtlCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> maxTtlCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> negativeTtlCaptor = ArgumentCaptor.forClass(Integer.class);
        DnsResolverUtil.applyJdkDnsCacheSettings(builder);
        verify(builder).ttl(minTtlCaptor.capture(), maxTtlCaptor.capture());
        verify(builder).negativeTtl(negativeTtlCaptor.capture());
        assertEquals(minTtlCaptor.getValue(), MIN_TTL);
        assertEquals(maxTtlCaptor.getValue(), TTL);
        assertEquals(negativeTtlCaptor.getValue(), NEGATIVE_TTL);
    }

    @Test
    public void testMaxTtl() {
        EventLoop eventLoop = Mockito.mock(EventLoop.class);
        DnsNameResolverBuilder dnsNameResolverBuilder = new DnsNameResolverBuilder(eventLoop);
        DnsResolverUtil.applyJdkDnsCacheSettings(dnsNameResolverBuilder);
        // If the maxTtl is <=0, it will throw IllegalArgumentException.
        try {
            dnsNameResolverBuilder.build();
        } catch (Exception ex) {
            Assert.assertFalse(ex instanceof IllegalArgumentException);
        }
    }
}
