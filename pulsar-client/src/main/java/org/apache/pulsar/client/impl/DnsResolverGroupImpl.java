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
package org.apache.pulsar.client.impl;

import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.DnsServerAddressStreamProvider;
import io.netty.resolver.dns.SequentialDnsServerAddressStreamProvider;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.util.netty.DnsResolverUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

/**
 * An abstraction to manage a group of Netty {@link AddressResolver} instances.
 * Uses {@link io.netty.resolver.dns.DnsAddressResolverGroup} to create the {@link AddressResolver} instance
 * since it contains a shared DNS cache and a solution to prevent cache stampede / thundering herds problem
 * when a DNS entry expires while the system is under high load.
 */
public class DnsResolverGroupImpl implements AutoCloseable {
    private final DnsAddressResolverGroup dnsAddressResolverGroup;

    public DnsResolverGroupImpl(EventLoopGroup eventLoopGroup, ClientConfigurationData conf) {
        Optional<InetSocketAddress> bindAddress = Optional.ofNullable(conf.getDnsLookupBindAddress())
                .map(addr -> new InetSocketAddress(addr, conf.getDnsLookupBindPort()));
        Optional<DnsServerAddressStreamProvider> dnsServerAddresses = Optional.ofNullable(conf.getDnsServerAddresses())
                .filter(Predicate.not(List::isEmpty))
                .map(SequentialDnsServerAddressStreamProvider::new);
        this.dnsAddressResolverGroup = createAddressResolverGroup(eventLoopGroup, bindAddress, dnsServerAddresses);
    }

    public DnsResolverGroupImpl(EventLoopGroup eventLoopGroup, Optional<InetSocketAddress> bindAddress,
                                Optional<DnsServerAddressStreamProvider> dnsServerAddresses) {
        this.dnsAddressResolverGroup = createAddressResolverGroup(eventLoopGroup, bindAddress, dnsServerAddresses);
    }

    private static DnsAddressResolverGroup createAddressResolverGroup(EventLoopGroup eventLoopGroup,
                                                                      Optional<InetSocketAddress> bindAddress,
                                                                      Optional<DnsServerAddressStreamProvider>
                                                                              dnsServerAddresses) {
        DnsNameResolverBuilder dnsNameResolverBuilder = createDnsNameResolverBuilder(eventLoopGroup);
        bindAddress.ifPresent(dnsNameResolverBuilder::localAddress);
        dnsServerAddresses.ifPresent(dnsNameResolverBuilder::nameServerProvider);

        return new DnsAddressResolverGroup(dnsNameResolverBuilder);
    }

    private static DnsNameResolverBuilder createDnsNameResolverBuilder(EventLoopGroup eventLoopGroup) {
        DnsNameResolverBuilder dnsNameResolverBuilder = new DnsNameResolverBuilder()
                .traceEnabled(true)
                .channelType(EventLoopUtil.getDatagramChannelClass(eventLoopGroup))
                .socketChannelType(EventLoopUtil.getClientSocketChannelClass(eventLoopGroup), true);
        DnsResolverUtil.applyJdkDnsCacheSettings(dnsNameResolverBuilder);
        return dnsNameResolverBuilder;
    }

    @Override
    public void close() {
        this.dnsAddressResolverGroup.close();
    }

    public AddressResolver<InetSocketAddress> createAddressResolver(EventLoopGroup eventLoopGroup) {
        return dnsAddressResolverGroup.getResolver(eventLoopGroup.next());
    }
}