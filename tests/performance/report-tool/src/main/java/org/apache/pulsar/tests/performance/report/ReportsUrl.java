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
package org.apache.pulsar.tests.performance.report;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * The HTTP URLs of the reports that {@link ReportsServer} serves, which the launcher prints beside the reports' files,
 * so that a report on another host opens with a click in the terminal. The base URL is the one configured, else the
 * server's address and port. A server bound to every interface, {@code 0.0.0.0}, is reached at the address of the
 * host's first network interface with an IPv4 address; a host with several interfaces configures the base URL
 * instead.
 */
public final class ReportsUrl {
    public static final String DEFAULT_BIND_ADDRESS = "127.0.0.1";
    public static final int DEFAULT_PORT = 8000;
    private static final String INDEX = "index.html";

    /**
     * A network interface, as far as choosing an address goes.
     *
     * @param index the interface's index, which orders the interfaces as the operating system created them
     */
    record Interface(int index, boolean up, boolean loopback, List<InetAddress> addresses) {
    }

    private ReportsUrl() {
    }

    /**
     * The base URL of the reports root, ending in {@code /}.
     *
     * @param configuredBaseUrl the configured base URL, or null or blank to derive it from the server's address
     * @param bindAddress the address that the server binds to, or null or blank for {@link #DEFAULT_BIND_ADDRESS}
     * @param port the port that the server listens on
     */
    public static String baseUrl(String configuredBaseUrl, String bindAddress, int port) {
        if (configuredBaseUrl != null && !configuredBaseUrl.isBlank()) {
            String baseUrl = configuredBaseUrl.trim();
            return baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        }
        String address = bindAddress == null || bindAddress.isBlank() ? DEFAULT_BIND_ADDRESS : bindAddress.trim();
        String host = isWildcard(address) ? firstIpv4Address(interfaces()).orElse(DEFAULT_BIND_ADDRESS) : address;
        // An IPv6 address is bracketed in a URL
        return "http://" + (host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host) + ":" + port + "/";
    }

    /**
     * The URL of a report's page or directory under the reports root: a directory's {@code index.html}, which the
     * server opens for the directory, is left out.
     *
     * @return the URL, or empty when the page isn't under the reports root
     */
    public static Optional<String> url(String baseUrl, Path reportsRoot, Path page) {
        Path root = reportsRoot.toAbsolutePath().normalize();
        Path file = page.toAbsolutePath().normalize();
        if (!file.startsWith(root) || file.equals(root)) {
            return Optional.empty();
        }
        boolean index = file.getFileName().toString().equals(INDEX);
        String relative = root.relativize(index ? file.getParent() : file).toString().replace('\\', '/');
        if (index && !relative.isEmpty()) {
            relative += "/";
        }
        try {
            // Escapes what a path may not hold, such as spaces
            return Optional.of(baseUrl + new URI(null, null, relative, null).toASCIIString());
        } catch (URISyntaxException e) {
            return Optional.empty();
        }
    }

    /** Whether an address is the wildcard address that binds to every interface. */
    public static boolean isWildcard(String address) {
        return address.equals("0.0.0.0") || address.equals("::") || address.equals("[::]");
    }

    /** The IPv4 address of the first interface, by index, that is up and isn't the loopback interface. */
    static Optional<String> firstIpv4Address(List<Interface> interfaces) {
        return interfaces.stream()
                .filter(networkInterface -> networkInterface.up() && !networkInterface.loopback())
                .sorted(Comparator.comparingInt(Interface::index))
                .flatMap(networkInterface -> networkInterface.addresses().stream())
                .filter(address -> address instanceof Inet4Address && !address.isLoopbackAddress()
                        && !address.isLinkLocalAddress() && !address.isAnyLocalAddress())
                .map(InetAddress::getHostAddress)
                .findFirst();
    }

    private static List<Interface> interfaces() {
        try {
            return NetworkInterface.networkInterfaces().map(networkInterface -> {
                try {
                    return new Interface(networkInterface.getIndex(), networkInterface.isUp(),
                            networkInterface.isLoopback(), Collections.list(networkInterface.getInetAddresses()));
                } catch (SocketException e) {
                    return new Interface(networkInterface.getIndex(), false, false, List.of());
                }
            }).toList();
        } catch (SocketException e) {
            return List.of();
        }
    }
}
