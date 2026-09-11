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
package org.apache.pulsar.client.api;

import java.net.InetSocketAddress;

/**
 * Configuration interface for DNS resolver settings.
 */
public interface DnsResolverConfig {
    /**
     * Sets the minimum TTL for DNS records.
     * Defaults to 0.
     *
     * @param minTtl the minimum TTL value
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig minTtl(int minTtl);

    /**
     * Sets the maximum TTL for DNS records.
     * Defaults to JDK level setting.
     *
     * @param maxTtl the maximum TTL value
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig maxTtl(int maxTtl);

    /**
     * Sets the TTL for negative DNS responses.
     * Defaults to JDK level setting.
     *
     * @param negativeTtl the negative TTL value
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig negativeTtl(int negativeTtl);

    /**
     * Sets the query timeout in milliseconds.
     * Defaults to the OS level setting.
     *
     * @param queryTimeoutMillis the timeout value in milliseconds
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig queryTimeoutMillis(long queryTimeoutMillis);

    /**
     * Enables or disables TCP fallback for DNS queries.
     * Defaults to true.
     *
     * @param tcpFallbackEnabled true to enable TCP fallback, false to disable
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig tcpFallbackEnabled(boolean tcpFallbackEnabled);

    /**
     * Enables or disables TCP fallback on timeout for DNS queries.
     * Defaults to true.
     *
     * @param tcpFallbackOnTimeoutEnabled true to enable TCP fallback on timeout, false to disable
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig tcpFallbackOnTimeoutEnabled(boolean tcpFallbackOnTimeoutEnabled);

    /**
     * Sets the ndots value for DNS resolution. Set this to 1 to disable the use of DNS search domains.
     * Defaults to the OS level configuration.
     *
     * @param ndots the ndots value
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig ndots(int ndots);

    /**
     * Sets the search domains for DNS resolution.
     * Defaults to the OS level configuration.
     *
     * @param searchDomains collection of search domains
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig searchDomains(Iterable<String> searchDomains);

    /**
     * Sets the local bind address and port for DNS lookup client.
     * By default any available port is used. This setting is mainly in cases where it's necessary to bind
     * the DNS client to a specific address and optionally also the port.
     *
     * @param localAddress the socket address (address and port) to bind the DNS client to
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig localAddress(InetSocketAddress localAddress);

    /**
     * Sets the DNS server addresses to be used for DNS lookups.
     * Defaults to the OS level configuration.
     *
     * @param addresses collection of DNS server socket addresses (address and port)
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig serverAddresses(Iterable<InetSocketAddress> addresses);

    /**
     * Enable detailed trace information for resolution failure exception messages.
     * Defaults to true.
     *
     * @param traceEnabled true to enable tracing, false to disable
     * @return the DNS resolver configuration instance for chained calls
     */
    DnsResolverConfig traceEnabled(boolean traceEnabled);
}
