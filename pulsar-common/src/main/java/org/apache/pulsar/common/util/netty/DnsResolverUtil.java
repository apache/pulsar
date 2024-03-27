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

import io.netty.resolver.dns.DnsNameResolverBuilder;
import java.security.Security;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DnsResolverUtil {

    private static final String CACHE_POLICY_PROP = "networkaddress.cache.ttl";
    private static final String CACHE_POLICY_PROP_FALLBACK = "sun.net.inetaddr.ttl";
    private static final String NEGATIVE_CACHE_POLICY_PROP = "networkaddress.cache.negative.ttl";
    private static final String NEGATIVE_CACHE_POLICY_PROP_FALLBACK = "sun.net.inetaddr.negative.ttl";
    /* default ttl value from sun.net.InetAddressCachePolicy.DEFAULT_POSITIVE， which is used when no security manager
     is used */
    private static final int JDK_DEFAULT_TTL = 30;
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
            String ttlStr = Security.getProperty(CACHE_POLICY_PROP);
            if (ttlStr == null) {
                // Compatible with sun.net.inetaddr.ttl settings
                ttlStr = System.getProperty(CACHE_POLICY_PROP_FALLBACK);
            }
            String negativeTtlStr = Security.getProperty(NEGATIVE_CACHE_POLICY_PROP);
            if (negativeTtlStr == null) {
                // Compatible with sun.net.inetaddr.negative.ttl settings
                negativeTtlStr = System.getProperty(NEGATIVE_CACHE_POLICY_PROP_FALLBACK);
            }
            ttl = Optional.ofNullable(ttlStr)
                    .map(Integer::decode)
                    .filter(i -> i > 0)
                    .orElseGet(() -> {
                        if (System.getSecurityManager() == null) {
                            return JDK_DEFAULT_TTL;
                        }
                        return DEFAULT_TTL;
                    });

            negativeTtl = Optional.ofNullable(negativeTtlStr)
                    .map(Integer::decode)
                    .filter(i -> i >= 0)
                    .orElse(DEFAULT_NEGATIVE_TTL);
        } catch (NumberFormatException e) {
            log.warn("Cannot get DNS TTL settings", e);
        }
        TTL = ttl;
        NEGATIVE_TTL = negativeTtl;
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
