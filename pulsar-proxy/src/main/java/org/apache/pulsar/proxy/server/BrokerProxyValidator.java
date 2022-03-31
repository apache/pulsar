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

package org.apache.pulsar.proxy.server;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv6.IPv6Address;
import io.netty.resolver.AddressResolver;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.NettyFutureUtil;

@Slf4j
public class BrokerProxyValidator {
    private static final String SEPARATOR = "\\s*,\\s*";
    private static final String ALLOW_ANY = "*";
    private final int[] allowedTargetPorts;
    private final boolean allowAnyTargetPort;
    private final List<IPAddress> allowedIPAddresses;
    private final boolean allowAnyIPAddress;
    private final AddressResolver<InetSocketAddress> inetSocketAddressResolver;
    private final List<Pattern> allowedHostNames;
    private final boolean allowAnyHostName;

    public BrokerProxyValidator(AddressResolver<InetSocketAddress> inetSocketAddressResolver, String allowedHostNames,
                                String allowedIPAddresses, String allowedTargetPorts) {
        this.inetSocketAddressResolver = inetSocketAddressResolver;
        List<String> allowedHostNamesStrings = parseCommaSeparatedConfigValue(allowedHostNames);
        if (allowedHostNamesStrings.contains(ALLOW_ANY)) {
            this.allowAnyHostName = true;
            this.allowedHostNames = Collections.emptyList();
        } else {
            this.allowAnyHostName = false;
            this.allowedHostNames = allowedHostNamesStrings.stream()
                    .map(BrokerProxyValidator::parseWildcardPattern).collect(Collectors.toList());
        }
        List<String> allowedIPAddressesStrings = parseCommaSeparatedConfigValue(allowedIPAddresses);
        if (allowedIPAddressesStrings.contains(ALLOW_ANY)) {
            allowAnyIPAddress = true;
            this.allowedIPAddresses = Collections.emptyList();
        } else {
            allowAnyIPAddress = false;
            this.allowedIPAddresses = allowedIPAddressesStrings.stream().map(IPAddressString::new)
                    .filter(ipAddressString -> {
                        if (ipAddressString.isValid()) {
                            return true;
                        } else {
                            throw new IllegalArgumentException("Invalid IP address filter '" + ipAddressString + "'",
                                    ipAddressString.getAddressStringException());
                        }
                    }).map(IPAddressString::getAddress)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        List<String> allowedTargetPortsStrings = parseCommaSeparatedConfigValue(allowedTargetPorts);
        if (allowedTargetPortsStrings.contains(ALLOW_ANY)) {
            allowAnyTargetPort = true;
            this.allowedTargetPorts = new int[0];
        } else {
            allowAnyTargetPort = false;
            this.allowedTargetPorts =
                    allowedTargetPortsStrings.stream().mapToInt(Integer::parseInt).toArray();
        }
    }

    private static Pattern parseWildcardPattern(String wildcardPattern) {
        String regexPattern =
                Collections.list(new StringTokenizer(wildcardPattern, "*", true))
                        .stream()
                        .map(String::valueOf)
                        .map(token -> {
                            if ("*".equals(token)) {
                                return ".*";
                            } else {
                                return Pattern.quote(token);
                            }
                        }).collect(Collectors.joining());
        return Pattern.compile(
                "^" + regexPattern + "$",
                Pattern.CASE_INSENSITIVE);
    }

    private static List<String> parseCommaSeparatedConfigValue(String configValue) {
        return Arrays.stream(configValue.split(SEPARATOR)).map(String::trim).filter(s -> s.length() > 0)
                .collect(Collectors.toList());
    }

    public CompletableFuture<InetSocketAddress> resolveAndCheckTargetAddress(String hostAndPort) {
        int pos = hostAndPort.lastIndexOf(':');
        String host = hostAndPort.substring(0, pos);
        int port = Integer.parseInt(hostAndPort.substring(pos + 1));
        if (!isPortAllowed(port)) {
            return FutureUtil.failedFuture(
                    new TargetAddressDeniedException("Given port in '" + hostAndPort + "' isn't allowed."));
        } else if (!isHostAllowed(host)) {
            return FutureUtil.failedFuture(
                    new TargetAddressDeniedException("Given host in '" + hostAndPort + "' isn't allowed."));
        } else {
            return NettyFutureUtil.toCompletableFuture(
                            inetSocketAddressResolver.resolve(InetSocketAddress.createUnresolved(host, port)))
                    .thenCompose(resolvedAddress -> {
                        CompletableFuture<InetSocketAddress> result = new CompletableFuture();
                        if (isIPAddressAllowed(resolvedAddress)) {
                            result.complete(resolvedAddress);
                        } else {
                            result.completeExceptionally(new TargetAddressDeniedException(
                                    "The IP address of the given host and port '" + hostAndPort + "' isn't allowed."));
                        }
                        return result;
                    });
        }
    }

    private boolean isPortAllowed(int port) {
        if (allowAnyTargetPort) {
            return true;
        }
        for (int allowedPort : allowedTargetPorts) {
            if (allowedPort == port) {
                return true;
            }
        }
        return false;
    }

    private boolean isIPAddressAllowed(InetSocketAddress resolvedAddress) {
        if (allowAnyIPAddress) {
            return true;
        }
        byte[] addressBytes = resolvedAddress.getAddress().getAddress();
        IPAddress candidateAddress =
                addressBytes.length == 4 ? new IPv4Address(addressBytes) : new IPv6Address(addressBytes);
        for (IPAddress allowedAddress : allowedIPAddresses) {
            if (allowedAddress.contains(candidateAddress)) {
                return true;
            }
        }
        return false;
    }

    private boolean isHostAllowed(String host) {
        if (allowAnyHostName) {
            return true;
        }
        boolean matched = false;
        for (Pattern allowedHostName : allowedHostNames) {
            if (allowedHostName.matcher(host).matches()) {
                matched = true;
                break;
            }
        }
        return matched;
    }
}
