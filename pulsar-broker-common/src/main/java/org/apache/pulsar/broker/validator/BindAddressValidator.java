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
package org.apache.pulsar.broker.validator;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.common.configuration.BindAddress;

/**
 * Validates bind address configurations.
 */
public class BindAddressValidator {

    private static final Pattern BIND_ADDRESSES_PATTERN = Pattern.compile("(?<name>[^:]+):(?<url>.+)$");

    /**
     * Validate the configuration of `bindAddresses`.
     *
     * <p>Bindings derived from the legacy port properties (`brokerServicePort`, `brokerServicePortTls`,
     * `webServicePort`, `webServicePortTls`) are associated with the listener identified by
     * `internalListenerName` and merged with the entries declared in `bindAddresses`. Exact duplicates
     * (same `listener:scheme://ip:port`) are tolerated. Two failure modes are rejected:
     * <ol>
     *   <li>the same {@code scheme://ip:port} mapped to different listener names, and
     *   <li>the same {@code ip:port} mapped to two different protocol schemes — because a TCP socket
     *       can only be bound by one process, an IP+port can carry at most one scheme.
     * </ol>
     *
     * @param config the pulsar broker configuration.
     * @param schemes a filter on the schemes of the bind addresses, or null to not apply a filter.
     * @return a list of bind addresses.
     */
    public static List<BindAddress> validateBindAddresses(ServiceConfiguration config, Collection<String> schemes) {
        String internalListenerName = StringUtils.defaultIfBlank(config.getInternalListenerName(),
                ServiceConfiguration.DEFAULT_INTERNAL_LISTENER_NAME);

        // migrate the legacy port-based configuration to bind addresses tagged with the internal listener name
        List<BindAddress> addresses = migrateBindAddresses(config, internalListenerName);

        // parse the list of additional bind addresses; trim whitespace around the comma-separated
        // entries so configurations split across multiple lines or padded for readability are accepted
        Arrays.stream(StringUtils.split(StringUtils.defaultString(config.getBindAddresses()), ","))
                .map(StringUtils::trim)
                .filter(StringUtils::isNotEmpty)
                .map(s -> {
                    Matcher m = BIND_ADDRESSES_PATTERN.matcher(s);
                    if (!m.matches()) {
                        throw new IllegalArgumentException("bindAddresses: malformed: " + s);
                    }
                    return m;
                })
                .map(m -> {
                    String name = StringUtils.trim(m.group("name"));
                    MultipleListenerValidator.validateListenerName(name);
                    return new BindAddress(name, URI.create(StringUtils.trim(m.group("url"))));
                })
                .forEach(addresses::add);

        // apply the filter
        if (schemes != null) {
            addresses.removeIf(a -> !schemes.contains(a.getAddress().getScheme()));
        }

        // Deduplicate by full URI (scheme + ip + port). Tolerate exact duplicates (same URI and
        // same listener name) so that a user's bindAddresses entry that matches a migrated binding
        // is accepted; reject same URI assigned to different listener names.
        Map<URI, BindAddress> uniqueBindAddresses = new LinkedHashMap<>();
        for (BindAddress addr : addresses) {
            BindAddress existing = uniqueBindAddresses.get(addr.getAddress());
            if (existing == null) {
                uniqueBindAddresses.put(addr.getAddress(), addr);
            } else if (Objects.equals(existing.getListenerName(), addr.getListenerName())) {
                // exact duplicate, tolerate
                continue;
            } else {
                throw new IllegalArgumentException("bindAddresses: conflicting listener names for "
                        + addr.getAddress() + ": `" + existing.getListenerName() + "` and `"
                        + addr.getListenerName() + "`");
            }
        }

        // ip:port uniqueness across protocol schemes. A TCP socket can only be bound by one
        // listener+scheme combination, so two bindings that share host:port but differ in scheme
        // (e.g. pulsar://0.0.0.0:8080 and http://0.0.0.0:8080) cannot both be active. Port 0 is
        // skipped because it means "OS-assigned ephemeral port" — the kernel will hand out a unique
        // port to each socket, so two port-0 entries with the same IP cannot actually collide.
        Map<String, BindAddress> uniqueIpPort = new LinkedHashMap<>();
        for (BindAddress addr : uniqueBindAddresses.values()) {
            if (addr.getAddress().getPort() == 0) {
                continue;
            }
            String ipPort = MultipleListenerValidator.formatHostPort(addr.getAddress());
            BindAddress prior = uniqueIpPort.putIfAbsent(ipPort, addr);
            if (prior != null) {
                throw new IllegalArgumentException("bindAddresses: ip:port `" + ipPort
                        + "` is bound by two schemes: `" + prior.getListenerName() + ":"
                        + prior.getAddress().getScheme() + "` and `" + addr.getListenerName() + ":"
                        + addr.getAddress().getScheme() + "`; an ip:port can carry only one scheme");
            }
        }

        return new ArrayList<>(uniqueBindAddresses.values());
    }

    /**
     * Generates bind addresses based on legacy configuration properties. The synthesized bindings are
     * tagged with the {@code internalListenerName} so that {@link MultipleListenerValidator} and
     * downstream code can correlate them with the internal advertised listener.
     */
    private static List<BindAddress> migrateBindAddresses(ServiceConfiguration config, String internalListenerName) {
        List<BindAddress> addresses = new ArrayList<>(4);
        String bindAddress = config.getBindAddress();
        if (config.getBrokerServicePort().isPresent()) {
            addresses.add(new BindAddress(internalListenerName,
                    URI.create(ServiceConfigurationUtils.brokerUrl(bindAddress, config.getBrokerServicePort().get()))));
        }
        if (config.getBrokerServicePortTls().isPresent()) {
            addresses.add(new BindAddress(internalListenerName, URI.create(
                    ServiceConfigurationUtils.brokerUrlTls(bindAddress, config.getBrokerServicePortTls().get()))));
        }
        if (config.getWebServicePort().isPresent()) {
            addresses.add(new BindAddress(internalListenerName, URI.create(
                    ServiceConfigurationUtils.webServiceUrl(bindAddress, config.getWebServicePort().get()))));
        }
        if (config.getWebServicePortTls().isPresent()) {
            addresses.add(new BindAddress(internalListenerName, URI.create(
                    ServiceConfigurationUtils.webServiceUrlTls(bindAddress, config.getWebServicePortTls().get()))));
        }
        return addresses;
    }
}
