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

import io.netty.resolver.AddressResolver;
import io.netty.resolver.DefaultNameResolver;
import io.netty.resolver.InetNameResolver;
import io.netty.resolver.InetSocketAddressResolver;
import io.netty.resolver.NameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.Security;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;

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
                        try {
                            if (System.getSecurityManager() == null) {
                                return JDK_DEFAULT_TTL;
                            }
                        } catch (Throwable t) {
                            log.warn("Cannot use current logic to resolve JDK default DNS TTL settings. Use "
                                            + "sun.net.inetaddr.ttl and sun.net.inetaddr.negative.ttl system "
                                            + "properties for setting default values for DNS TTL settings. {}",
                                    t.getMessage());
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

    public static int getDefaultMinTTL() {
        return MIN_TTL;
    }

    public static int getDefaultTTL() {
        return TTL;
    }

    public static int getDefaultNegativeTTL() {
        return NEGATIVE_TTL;
    }

    /**
     * Extract the underlying Netty NameResolver from an AddressResolver instance or creates an adapter as the
     * fallback. If null is passed in a default Netty NameResolver will be returned which delegates to the
     * blocking JDK DNS resolver.
     *
     * @param addressResolver Netty AddressResolver instance or null
     * @return Netty NameResolver instance
     */
    @SuppressWarnings("unchecked")
    public static NameResolver<InetAddress> adaptToNameResolver(AddressResolver<InetSocketAddress> addressResolver) {
        if (addressResolver == null) {
            return new DefaultNameResolver(ImmediateEventExecutor.INSTANCE);
        }
        // Use reflection to extract underlying Netty NameResolver instance.
        if (InetSocketAddressResolver.class.isInstance(addressResolver)) {
            try {
                Field nameResolverField =
                        FieldUtils.getDeclaredField(InetSocketAddressResolver.class, "nameResolver", true);
                if (nameResolverField != null) {
                    return (NameResolver<InetAddress>) FieldUtils.readField(nameResolverField, addressResolver);
                } else {
                    log.warn("Could not find nameResolver Field in InetSocketAddressResolver instance.");
                }
            } catch (Throwable t) {
                log.warn("Failed to extract NameResolver from InetSocketAddressResolver instance. {}", t.getMessage());
            }
        }
        // fallback to use an adapter if reflection fails
        log.info("Creating NameResolver adapter that wraps an AddressResolver instance.");
        return createNameResolverAdapter(addressResolver, ImmediateEventExecutor.INSTANCE);
    }

    /**
     * Creates a NameResolver adapter that wraps an AddressResolver instance.
     * <p>
     * This adapter is necessary because Netty doesn't provide a direct implementation for converting
     * between AddressResolver and NameResolver, while AsyncHttpClient specifically requires a NameResolver.
     * The adapter handles the resolution of hostnames to IP addresses by delegating to the underlying
     * AddressResolver.
     *
     * @param addressResolver the AddressResolver instance to adapt, handling InetSocketAddress resolution
     * @param executor        the EventExecutor to be used for executing resolution tasks
     * @return a NameResolver instance that wraps the provided AddressResolver
     */
    static NameResolver<InetAddress> createNameResolverAdapter(
            AddressResolver<InetSocketAddress> addressResolver, EventExecutor executor) {
        return new InetNameResolver(executor) {
            @Override
            protected void doResolve(String inetHost, Promise<InetAddress> promise) throws Exception {
                Promise<InetSocketAddress> delegatedPromise = executor().newPromise();
                addressResolver.resolve(InetSocketAddress.createUnresolved(inetHost, 1), delegatedPromise);
                delegatedPromise.addListener(new GenericFutureListener<Promise<InetSocketAddress>>() {
                    @Override
                    public void operationComplete(Promise<InetSocketAddress> future) throws Exception {
                        if (future.isSuccess()) {
                            promise.setSuccess(future.get().getAddress());
                        } else {
                            promise.setFailure(future.cause());
                        }
                    }
                });
            }

            @Override
            protected void doResolveAll(String inetHost, Promise<List<InetAddress>> promise) throws Exception {
                Promise<List<InetSocketAddress>> delegatedPromise = executor().newPromise();
                addressResolver.resolveAll(InetSocketAddress.createUnresolved(inetHost, 1), delegatedPromise);
                delegatedPromise.addListener(new GenericFutureListener<Promise<List<InetSocketAddress>>>() {
                    @Override
                    public void operationComplete(Promise<List<InetSocketAddress>> future) throws Exception {
                        if (future.isSuccess()) {
                            promise.setSuccess(future.get().stream().map(InetSocketAddress::getAddress).toList());
                        } else {
                            promise.setFailure(future.cause());
                        }
                    }
                });
            }
        };
    }
}
