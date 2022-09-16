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
package org.apache.pulsar.common.util.netty;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import java.security.Security;
import org.testng.annotations.Test;

public class DnsResolverUtilTest {
    static {
        Security.setProperty("networkaddress.cache.ttl", "31");
        Security.setProperty("networkaddress.cache.negative.ttl", "11");
    }

    private final DnsNameResolverBuilder builder = mock(DnsNameResolverBuilder.class);

    @Test
    public void test() {
        DnsResolverUtil.applyJdkDnsCacheSettings(builder);

        verify(builder).ttl(DnsResolverUtil.MIN_TTL, 31);
        verify(builder).negativeTtl(11);
    }
}
