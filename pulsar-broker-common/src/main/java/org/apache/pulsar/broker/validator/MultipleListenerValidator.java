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
package org.apache.pulsar.broker.validator;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;

/**
 * Validates multiple listener address configurations.
 */
public final class MultipleListenerValidator {

    /**
     * Validate the configuration of `advertisedListeners`, `internalListenerName`.
     * 1. `advertisedListeners` consists of a comma-separated list of endpoints.
     * 2. Each endpoint consists of a listener name and an associated address (`listener:scheme://host:port`).
     * 3. A listener name may be repeated to define both a non-TLS and a TLS endpoint.
     * 4. Duplicate definitions are disallowed.
     * 5. If `internalListenerName` is absent, set it to the first listener defined in `advertisedListeners`.
     * @param config the pulsar broker configure.
     * @return
     */
    public static Map<String, AdvertisedListener> validateAndAnalysisAdvertisedListener(ServiceConfiguration config) {
        if (StringUtils.isBlank(config.getAdvertisedListeners())) {
            return Collections.emptyMap();
        }
        Optional<String> firstListenerName = Optional.empty();
        Map<String, List<String>> listeners = new LinkedHashMap<>();
        for (final String str : StringUtils.split(config.getAdvertisedListeners(), ",")) {
            int index = str.indexOf(":");
            if (index <= 0) {
                throw new IllegalArgumentException("the configure entry `advertisedListeners` is invalid. because "
                        + str + " do not contain listener name");
            }
            String listenerName = StringUtils.trim(str.substring(0, index));
            if (!firstListenerName.isPresent()) {
                firstListenerName = Optional.of(listenerName);
            }
            String value = StringUtils.trim(str.substring(index + 1));
            listeners.computeIfAbsent(listenerName, k -> new ArrayList<>(2));
            listeners.get(listenerName).add(value);
        }
        if (StringUtils.isBlank(config.getInternalListenerName())) {
            config.setInternalListenerName(firstListenerName.get());
        }
        if (!listeners.containsKey(config.getInternalListenerName())) {
            throw new IllegalArgumentException("the `advertisedListeners` configure do not contain "
                    + "`internalListenerName` entry");
        }
        final Map<String, AdvertisedListener> result = new LinkedHashMap<>();
        final Map<String, Set<String>> reverseMappings = new LinkedHashMap<>();
        for (final Map.Entry<String, List<String>> entry : listeners.entrySet()) {
            if (entry.getValue().size() > 2) {
                throw new IllegalArgumentException("there are redundant configure for listener `" + entry.getKey()
                        + "`");
            }
            URI pulsarAddress = null, pulsarSslAddress = null, pulsarHttpAddress = null, pulsarHttpsAddress = null;
            for (final String strUri : entry.getValue()) {
                try {
                    URI uri = URI.create(strUri);
                    if (StringUtils.equalsIgnoreCase(uri.getScheme(), "pulsar")) {
                        if (pulsarAddress == null) {
                            pulsarAddress = uri;
                        } else {
                            throw new IllegalArgumentException("there are redundant configure for listener `"
                                    + entry.getKey() + "`");
                        }
                    } else if (StringUtils.equalsIgnoreCase(uri.getScheme(), "pulsar+ssl")) {
                        if (pulsarSslAddress == null) {
                            pulsarSslAddress = uri;
                        } else {
                            throw new IllegalArgumentException("there are redundant configure for listener `"
                                    + entry.getKey() + "`");
                        }
                    } else if (StringUtils.equalsIgnoreCase(uri.getScheme(), "http")) {
                        if (pulsarHttpAddress == null) {
                            pulsarHttpAddress = uri;
                        } else {
                            throw new IllegalArgumentException("there are redundant configure for listener `"
                                    + entry.getKey() + "`");
                        }
                    } else if (StringUtils.equalsIgnoreCase(uri.getScheme(), "https")) {
                        if (pulsarHttpsAddress == null) {
                            pulsarHttpsAddress = uri;
                        } else {
                            throw new IllegalArgumentException("there are redundant configure for listener `"
                                    + entry.getKey() + "`");
                        }
                    }

                    String hostPort = String.format("%s:%d", uri.getHost(), uri.getPort());
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

}
