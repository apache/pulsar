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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * the validator for pulsar multiple  listeners.
 */
public final class MultipleListenerValidator {

    /**
     * validate the configure of `advertisedListeners`, `internalListenerName`, `advertisedAddress`.
     * 1. `advertisedListeners` and `advertisedAddress` must not appear together.
     * 2. the listener name in `advertisedListeners` must not duplicate.
     * 3. user can not assign same 'host:port' to different listener.
     * 4. if `internalListenerName` is absent, the first `listener` in the `advertisedListeners` will be the `internalListenerName`.
     * 5. if pulsar do not specify `brokerServicePortTls`, should only contain one entry of `pulsar://` per listener name.
     * @param config the pulsar broker configure.
     * @return
     */
    public static Map<String, AdvertisedListener> validateAndAnalysisAdvertisedListener(ServiceConfiguration config) {
        if (StringUtils.isNotBlank(config.getAdvertisedListeners()) && StringUtils.isNotBlank(config.getAdvertisedAddress())) {
            throw new IllegalArgumentException("`advertisedListeners` and `advertisedAddress` must not appear together");
        }
        if (StringUtils.isBlank(config.getAdvertisedListeners())) {
            return Collections.emptyMap();
        }
        Optional<String> firstListenerName = Optional.empty();
        Map<String, List<String>> listeners = Maps.newHashMap();
        for (final String str : StringUtils.split(config.getAdvertisedListeners(), ",")) {
            int index = str.indexOf(":");
            if (index <= 0) {
                throw new IllegalArgumentException("the configure entry `advertisedListeners` is invalid. because " +
                        str + " do not contain listener name");
            }
            String listenerName = StringUtils.trim(str.substring(0, index));
            if (!firstListenerName.isPresent()) {
                firstListenerName = Optional.of(listenerName);
            }
            String value = StringUtils.trim(str.substring(index + 1));
            listeners.computeIfAbsent(listenerName, k -> Lists.newArrayListWithCapacity(2));
            listeners.get(listenerName).add(value);
        }
        if (StringUtils.isBlank(config.getInternalListenerName())) {
            config.setInternalListenerName(firstListenerName.get());
        }
        if (!listeners.containsKey(config.getInternalListenerName())) {
            throw new IllegalArgumentException("the `advertisedListeners` configure do not contain `internalListenerName` entry");
        }
        final Map<String, AdvertisedListener> result = Maps.newHashMap();
        final Map<String, Set<String>> reverseMappings = Maps.newHashMap();
        for (final Map.Entry<String, List<String>> entry : listeners.entrySet()) {
            if (entry.getValue().size() > 2) {
                throw new IllegalArgumentException("there are redundant configure for listener `" + entry.getKey() + "`");
            }
            URI pulsarAddress = null, pulsarSslAddress = null;
            for (final String strUri : entry.getValue()) {
                try {
                    URI uri = URI.create(strUri);
                    if (StringUtils.equalsIgnoreCase(uri.getScheme(), "pulsar")) {
                        if (pulsarAddress == null) {
                            pulsarAddress = uri;
                        } else {
                            throw new IllegalArgumentException("there are redundant configure for listener `" + entry.getKey() + "`");
                        }
                    } else if (StringUtils.equalsIgnoreCase(uri.getScheme(), "pulsar+ssl")) {
                        if (pulsarSslAddress == null) {
                            pulsarSslAddress = uri;
                        } else {
                            throw new IllegalArgumentException("there are redundant configure for listener `" + entry.getKey() + "`");
                        }
                    }
                    String hostPort = String.format("%s:%d", uri.getHost(), uri.getPort());
                    reverseMappings.computeIfAbsent(hostPort, k -> Sets.newTreeSet());
                    Set<String> sets = reverseMappings.computeIfAbsent(hostPort, k -> Sets.newTreeSet());
                    sets.add(entry.getKey());
                    if (sets.size() > 1) {
                        throw new IllegalArgumentException("must not specify `" + hostPort + "` to different listener.");
                    }
                } catch (Throwable cause) {
                    throw new IllegalArgumentException("the value " + strUri + " in the `advertisedListeners` configure is invalid");
                }
            }
            if (!config.getBrokerServicePortTls().isPresent()) {
                if (pulsarSslAddress != null) {
                    throw new IllegalArgumentException("If pulsar do not start ssl port, there is no need to configure " +
                            " `pulsar+ssl` in `" + entry.getKey() + "` listener.");
                }
            } else {
                if (pulsarSslAddress == null) {
                    throw new IllegalArgumentException("the `" + entry.getKey() + "` listener in the `advertisedListeners` "
                            + " do not specify `pulsar+ssl` address.");
                }
            }
            if (pulsarAddress == null) {
                throw new IllegalArgumentException("the `" + entry.getKey() + "` listener in the `advertisedListeners` "
                        + " do not specify `pulsar` address.");
            }
            result.put(entry.getKey(), AdvertisedListener.builder().brokerServiceUrl(pulsarAddress).brokerServiceUrlTls(pulsarSslAddress).build());
        }
        return result;
    }

}
