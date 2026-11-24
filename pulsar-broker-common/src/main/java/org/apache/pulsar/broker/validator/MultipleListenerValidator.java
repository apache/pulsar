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

import io.netty.util.NetUtil;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;

/**
 * Validates multiple listener address configurations.
 */
public final class MultipleListenerValidator {

    /** Allowed listener-name characters: ASCII letters, digits, underscore, hyphen. */
    private static final Pattern LISTENER_NAME_PATTERN = Pattern.compile("[A-Za-z0-9_-]+");

    /**
     * Format the host:port part of a URI for use as a uniqueness key and in error messages, wrapping
     * IPv6 literals in brackets so that the colon separator is unambiguous. {@link URI#getHost()} may
     * or may not include the brackets depending on the JDK, so they are stripped before the
     * {@link NetUtil#isValidIpV6Address} check.
     */
    static String formatHostPort(URI uri) {
        String host = uri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("host must not be null");
        }
        String unbracketed = host.startsWith("[") && host.endsWith("]")
                ? host.substring(1, host.length() - 1) : host;
        if (NetUtil.isValidIpV6Address(unbracketed)) {
            return "[" + unbracketed + "]:" + uri.getPort();
        }
        return host + ":" + uri.getPort();
    }

    /**
     * Validate a listener name. Listener names must be non-blank and contain only ASCII letters,
     * digits, underscore, and hyphen so they are safe to embed in URLs without encoding.
     *
     * @throws IllegalArgumentException if the name is null, blank, or contains disallowed characters.
     */
    public static void validateListenerName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("listener name must not be blank");
        }
        if (!LISTENER_NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("listener name `" + name + "` must contain only ASCII"
                    + " letters, digits, underscore, or hyphen");
        }
    }

    /**
     * Validate `advertisedListeners` and `internalListenerName`, returning the parsed listener map.
     * <p>
     * This method mutates the supplied {@link ServiceConfiguration}: when {@code internalListenerName}
     * is blank, it is written back with the resolved fallback value (the first parsed listener if any,
     * otherwise {@value ServiceConfiguration#DEFAULT_INTERNAL_LISTENER_NAME}) so that subsequent reads
     * from the config see the effective value.
     * <ol>
     * <li>`advertisedListeners` is a comma-separated list of endpoints in the form
     *     `listener:scheme://host:port`. Supported schemes are `pulsar`, `pulsar+ssl`, `http`, and `https`.
     * <li>A listener name may be repeated to define multiple endpoints (e.g. binary and HTTPS) for the
     *     same listener; duplicate definitions for the same scheme are rejected.
     * <li>`internalListenerName` identifies the listener used for cluster-internal broker-to-broker
     *     communication. It defaults to {@value ServiceConfiguration#DEFAULT_INTERNAL_LISTENER_NAME}.
     *     An internal listener entry is synthesized from the legacy `brokerServicePort`,
     *     `brokerServicePortTls`, `webServicePort`, and `webServicePortTls` properties; any URLs the
     *     user has explicitly declared for the internal listener in `advertisedListeners` take
     *     precedence and the legacy-port URLs only fill in the unspecified slots. This allows the
     *     internal listener to override individual URLs (e.g. a custom TLS hostname) while still
     *     benefiting from auto-population for the rest.
     * </ol>
     * @param config the pulsar broker configuration; updated in place when
     *               {@code internalListenerName} is blank.
     * @return the parsed and validated advertised listeners, keyed by listener name.
     */
    public static Map<String, AdvertisedListener> validateAndUpdateAdvertisedListeners(ServiceConfiguration config) {
        Map<String, AdvertisedListener> result = parseAdvertisedListeners(config);

        // Resolve `internalListenerName` if the user left it blank (null, empty, or whitespace).
        // For backward compatibility, prefer the first entry parsed from `advertisedListeners`;
        // otherwise fall back to the constant default ("internal"). The field's own default is
        // also "internal", so this branch is normally taken only when the user explicitly set the
        // property to an empty string.
        if (StringUtils.isBlank(config.getInternalListenerName())) {
            String fallback = result.isEmpty()
                    ? ServiceConfiguration.DEFAULT_INTERNAL_LISTENER_NAME
                    : result.keySet().iterator().next();
            config.setInternalListenerName(fallback);
        }
        String internalListenerName = config.getInternalListenerName();

        // Merge the legacy-port-derived internal listener URLs into any explicit configuration.
        // Explicit URLs in `advertisedListeners` take precedence; the legacy-port URLs fill in the
        // unspecified slots so that broker-to-broker communication keeps working without forcing the
        // user to redeclare every URL when they only want to override one.
        AdvertisedListener fromLegacyPorts = buildInternalListenerFromLegacyPorts(config);
        if (fromLegacyPorts != null) {
            AdvertisedListener explicit = result.get(internalListenerName);
            result.put(internalListenerName, mergeListeners(explicit, fromLegacyPorts));
        }

        if (!result.isEmpty() && !result.containsKey(internalListenerName)) {
            throw new IllegalArgumentException("the `advertisedListeners` configuration does not contain "
                    + "an entry for the internal listener `" + internalListenerName + "`, and the legacy "
                    + "port properties are not configured so an internal listener cannot be synthesized");
        }

        return result;
    }

    /**
     * Merges two {@link AdvertisedListener} entries. URLs from {@code override} take precedence; URLs
     * from {@code fallback} only fill in the slots that {@code override} leaves null. Either argument
     * may be null.
     */
    private static AdvertisedListener mergeListeners(AdvertisedListener override, AdvertisedListener fallback) {
        if (override == null) {
            return fallback;
        }
        if (fallback == null) {
            return override;
        }
        return AdvertisedListener.builder()
                .brokerServiceUrl(override.getBrokerServiceUrl() != null
                        ? override.getBrokerServiceUrl() : fallback.getBrokerServiceUrl())
                .brokerServiceUrlTls(override.getBrokerServiceUrlTls() != null
                        ? override.getBrokerServiceUrlTls() : fallback.getBrokerServiceUrlTls())
                .brokerHttpUrl(override.getBrokerHttpUrl() != null
                        ? override.getBrokerHttpUrl() : fallback.getBrokerHttpUrl())
                .brokerHttpsUrl(override.getBrokerHttpsUrl() != null
                        ? override.getBrokerHttpsUrl() : fallback.getBrokerHttpsUrl())
                .build();
    }

    private static Map<String, AdvertisedListener> parseAdvertisedListeners(ServiceConfiguration config) {
        if (StringUtils.isBlank(config.getAdvertisedListeners())) {
            return new LinkedHashMap<>();
        }
        Map<String, List<String>> listeners = new LinkedHashMap<>();
        // Trim whitespace around comma-separated entries so configurations split across multiple
        // lines or padded for readability are accepted, and skip empties.
        for (final String raw : StringUtils.split(config.getAdvertisedListeners(), ",")) {
            String str = StringUtils.trim(raw);
            if (StringUtils.isEmpty(str)) {
                continue;
            }
            int index = str.indexOf(":");
            if (index <= 0) {
                throw new IllegalArgumentException("the configure entry `advertisedListeners` is invalid. because "
                        + str + " do not contain listener name");
            }
            String listenerName = StringUtils.trim(str.substring(0, index));
            validateListenerName(listenerName);
            String value = StringUtils.trim(str.substring(index + 1));
            listeners.computeIfAbsent(listenerName, k -> new ArrayList<>(2));
            listeners.get(listenerName).add(value);
        }
        final Map<String, AdvertisedListener> result = new LinkedHashMap<>();
        final Map<String, Set<String>> reverseMappings = new LinkedHashMap<>();
        for (final Map.Entry<String, List<String>> entry : listeners.entrySet()) {
            if (entry.getValue().size() > 4) {
                throw new IllegalArgumentException("there are redundant configure for listener `" + entry.getKey()
                        + "`");
            }
            URI pulsarAddress = null, pulsarSslAddress = null, pulsarHttpAddress = null, pulsarHttpsAddress = null;
            for (final String strUri : entry.getValue()) {
                try {
                    URI uri = URI.create(strUri);
                    if ("pulsar".equalsIgnoreCase(uri.getScheme())) {
                        if (pulsarAddress == null) {
                            pulsarAddress = uri;
                        } else {
                            throw new IllegalArgumentException("there are redundant configure for listener `"
                                    + entry.getKey() + "`");
                        }
                    } else if ("pulsar+ssl".equalsIgnoreCase(uri.getScheme())) {
                        if (pulsarSslAddress == null) {
                            pulsarSslAddress = uri;
                        } else {
                            throw new IllegalArgumentException("there are redundant configure for listener `"
                                    + entry.getKey() + "`");
                        }
                    } else if ("http".equalsIgnoreCase(uri.getScheme())) {
                        if (pulsarHttpAddress == null) {
                            pulsarHttpAddress = uri;
                        } else {
                            throw new IllegalArgumentException("there are redundant configure for listener `"
                                    + entry.getKey() + "`");
                        }
                    } else if ("https".equalsIgnoreCase(uri.getScheme())) {
                        if (pulsarHttpsAddress == null) {
                            pulsarHttpsAddress = uri;
                        } else {
                            throw new IllegalArgumentException("there are redundant configure for listener `"
                                    + entry.getKey() + "`");
                        }
                    }

                    String hostPort = formatHostPort(uri);
                    Set<String> sets = reverseMappings.computeIfAbsent(hostPort, k -> new TreeSet<>());
                    sets.add(entry.getKey());
                    if (sets.size() > 1) {
                        throw new IllegalArgumentException("must not specify `" + hostPort
                                + "` to different listener.");
                    }
                } catch (Throwable cause) {
                    throw new IllegalArgumentException("the value " + strUri + " in the `advertisedListeners` "
                            + "configure is invalid", cause);
                }
            }
            result.put(entry.getKey(), AdvertisedListener.builder()
                    .brokerServiceUrl(pulsarAddress)
                    .brokerServiceUrlTls(pulsarSslAddress)
                    .brokerHttpUrl(pulsarHttpAddress)
                    .brokerHttpsUrl(pulsarHttpsAddress)
                    .build());
        }
        return result;
    }

    /**
     * Synthesize an {@link AdvertisedListener} for the internal listener from the legacy port
     * configuration (`brokerServicePort`, `brokerServicePortTls`, `webServicePort`,
     * `webServicePortTls`). Returns {@code null} if no binary port and no web port is set; the caller
     * is then responsible for raising an error if the internal listener is still missing after
     * parsing `advertisedListeners`.
     */
    private static AdvertisedListener buildInternalListenerFromLegacyPorts(ServiceConfiguration config) {
        boolean hasBinaryPort = config.getBrokerServicePort().isPresent()
                || config.getBrokerServicePortTls().isPresent();
        boolean hasWebPort = config.getWebServicePort().isPresent()
                || config.getWebServicePortTls().isPresent();
        if (!hasBinaryPort && !hasWebPort) {
            return null;
        }
        String host = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
        return AdvertisedListener.builder()
                .brokerServiceUrl(config.getBrokerServicePort()
                        .map(port -> URI.create(ServiceConfigurationUtils.brokerUrl(host, port))).orElse(null))
                .brokerServiceUrlTls(config.getBrokerServicePortTls()
                        .map(port -> URI.create(ServiceConfigurationUtils.brokerUrlTls(host, port))).orElse(null))
                .brokerHttpUrl(config.getWebServicePort()
                        .map(port -> URI.create(ServiceConfigurationUtils.webServiceUrl(host, port))).orElse(null))
                .brokerHttpsUrl(config.getWebServicePortTls()
                        .map(port -> URI.create(ServiceConfigurationUtils.webServiceUrlTls(host, port))).orElse(null))
                .build();
    }

    // Prevent instantiation
    private MultipleListenerValidator() {
    }
}
