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
package org.apache.pulsar.broker;

import static org.apache.commons.lang3.StringUtils.isBlank;
import io.netty.util.NetUtil;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import lombok.CustomLog;
import org.apache.pulsar.broker.validator.MultipleListenerValidator;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;

@CustomLog
public class ServiceConfigurationUtils {

    public static String getDefaultOrConfiguredAddress(String configuredAddress) {
        if (isBlank(configuredAddress)) {
            return unsafeLocalhostResolve();
        }
        return configuredAddress;
    }

    public static String unsafeLocalhostResolve() {
        try {
            // Get the fully qualified hostname
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException ex) {
            log.error().exception(ex).log(ex.getMessage());
            throw new IllegalStateException("Failed to resolve localhost name.", ex);
        }
    }

    /**
     * Get the address of Broker, first try to get it from AdvertisedAddress.
     * If it is not set, try to get the address set by advertisedListener.
     * If it is still not set, get it through InetAddress.getLocalHost().
     * @param configuration
     * @param ignoreAdvertisedListener Sometimes we can’t use the default key of AdvertisedListener,
     *                                 setting it to true can ignore AdvertisedListener.
     * @return
     */
    @Deprecated
    public static String getAppliedAdvertisedAddress(ServiceConfiguration configuration,
                                                     boolean ignoreAdvertisedListener) {
        Map<String, AdvertisedListener> result = MultipleListenerValidator
                .validateAndUpdateAdvertisedListeners(configuration);

        String advertisedAddress = configuration.getAdvertisedAddress();
        if (advertisedAddress != null) {
            return advertisedAddress;
        }

        AdvertisedListener advertisedListener = result.get(configuration.getInternalListenerName());
        if (advertisedListener != null && !ignoreAdvertisedListener) {
            // The listener may have been synthesized from a subset of the legacy ports, so any of
            // the four URLs may be null. Pick the first non-null URL to derive the host.
            URI url = firstNonNullUri(advertisedListener.getBrokerServiceUrl(),
                    advertisedListener.getBrokerServiceUrlTls(),
                    advertisedListener.getBrokerHttpUrl(),
                    advertisedListener.getBrokerHttpsUrl());
            if (url != null && url.getHost() != null) {
                return url.getHost();
            }
        }

        return getDefaultOrConfiguredAddress(advertisedAddress);
    }

    /**
     * Gets the internal advertised listener for broker-to-broker communication.
     * @return a non-null advertised listener
     */
    public static AdvertisedListener getInternalListener(ServiceConfiguration config, String protocol) {
        Map<String, AdvertisedListener> result = MultipleListenerValidator
                .validateAndUpdateAdvertisedListeners(config);
        AdvertisedListener internal = result.get(config.getInternalListenerName());
        if (internal == null || !internal.hasUriForProtocol(protocol)) {
            // Search for an advertised listener for same protocol
            for (AdvertisedListener l : result.values()) {
                if (l.hasUriForProtocol(protocol)) {
                    internal = l;
                    break;
                }
            }
        }

        if (internal == null) {
            // synthesize an advertised listener based on legacy configuration properties
            String host = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
            internal = AdvertisedListener.builder()
                    .brokerServiceUrl(createUriOrNull("pulsar", host, config.getBrokerServicePort()))
                    .brokerServiceUrlTls(createUriOrNull("pulsar+ssl", host, config.getBrokerServicePortTls()))
                    .build();

        }
        return internal;
    }

    private static URI createUriOrNull(String scheme, String ipOrHost, Optional<Integer> port) {
        return port.map(p -> URI.create(String.format("%s://%s:%d", scheme, formatHost(ipOrHost), p))).orElse(null);
    }

    /**
     * Wrap bare IPv6 literals in square brackets so they parse correctly as the host component of a
     * URL. IPv4 addresses and hostnames are returned unchanged, as is an IPv6 literal that is
     * already bracketed.
     */
    static String formatHost(String ipOrHost) {
        if (ipOrHost == null || ipOrHost.isEmpty()) {
            return ipOrHost;
        }
        if (ipOrHost.startsWith("[") && ipOrHost.endsWith("]")) {
            return ipOrHost;
        }
        if (NetUtil.isValidIpV6Address(ipOrHost)) {
            return "[" + ipOrHost + "]";
        }
        return ipOrHost;
    }

    private static URI firstNonNullUri(URI... uris) {
        for (URI uri : uris) {
            if (uri != null) {
                return uri;
            }
        }
        return null;
    }

    /**
     * Gets the web service address (hostname).
     */
    public static String getWebServiceAddress(ServiceConfiguration config) {
        return ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
    }

    public static String brokerUrl(String ipOrHost, int port) {
        return String.format("pulsar://%s:%d", formatHost(ipOrHost), port);
    }

    public static String brokerUrlTls(String ipOrHost, int port) {
        return String.format("pulsar+ssl://%s:%d", formatHost(ipOrHost), port);
    }

    public static String webServiceUrl(String ipOrHost, int port) {
        return String.format("http://%s:%d", formatHost(ipOrHost), port);
    }

    public static String webServiceUrlTls(String ipOrHost, int port) {
        return String.format("https://%s:%d", formatHost(ipOrHost), port);
    }
}
