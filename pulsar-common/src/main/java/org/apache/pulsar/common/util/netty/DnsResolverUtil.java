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

import io.netty.resolver.dns.DnsNameResolverBuilder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DnsResolverUtil {
    private static final int MIN_TTL = 0;
    private static final int TTL;
    private static final int NEGATIVE_TTL;

    // default TTL value when JDK setting is "forever" (-1)
    private static final int DEFAULT_TTL = 60;

    // default negative TTL value when JDK setting is "forever" (-1)
    private static final int DEFAULT_NEGATIVE_TTL = 10;

    static {
        int ttl = DEFAULT_TTL;
        int negativeTtl = DEFAULT_NEGATIVE_TTL;
        try {
            // use reflection to call sun.net.InetAddressCachePolicy's get and getNegative methods for getting
            // effective JDK settings for DNS caching
            Class<?> inetAddressCachePolicyClass = Class.forName("sun.net.InetAddressCachePolicy");
            Method getTTLMethod = inetAddressCachePolicyClass.getMethod("get");
            ttl = (Integer) getTTLMethod.invoke(null);
            Method getNegativeTTLMethod = inetAddressCachePolicyClass.getMethod("getNegative");
            negativeTtl = (Integer) getNegativeTTLMethod.invoke(null);
        } catch (NoSuchMethodException | ClassNotFoundException | InvocationTargetException
                 | IllegalAccessException e) {
            log.warn("Cannot get DNS TTL settings from sun.net.InetAddressCachePolicy class", e);
        }
        TTL = ttl <= 0 ? DEFAULT_TTL : ttl;
        NEGATIVE_TTL = negativeTtl < 0 ? DEFAULT_NEGATIVE_TTL : negativeTtl;
    }

    private DnsResolverUtil() {
        // utility class with static methods, prevent instantiation
    }

    /**
     * Configure Netty's {@link DnsNameResolverBuilder}'s ttl and negativeTtl to match the JDK's DNS caching settings.
     * If the JDK setting for TTL is forever (-1), the TTL will be set to 60 seconds.
     *
     * @param dnsNameResolverBuilder The Netty {@link DnsNameResolverBuilder} instance to apply the settings
     */
    public static void applyJdkDnsCacheSettings(DnsNameResolverBuilder dnsNameResolverBuilder) {
        dnsNameResolverBuilder.ttl(MIN_TTL, TTL);
        dnsNameResolverBuilder.negativeTtl(NEGATIVE_TTL);
    }
}
