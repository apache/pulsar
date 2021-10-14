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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.common.configuration.BindAddress;

/**
 * Validates bind address configurations.
 */
public class BindAddressValidator {

    private static final Pattern BIND_ADDRESSES_PATTERN = Pattern.compile("(?<name>\\w+):(?<url>.+)$");

    /**
     * Validate the configuration of `bindAddresses`.
     * @param config the pulsar broker configure.
     * @param schemes a filter on the schemes of the bind addresses, or null to not apply a filter.
     * @return a list of bind addresses.
     */
    public static List<BindAddress> validateBindAddresses(ServiceConfiguration config, Collection<String> schemes) {
        // migrate the existing configuration properties
        List<BindAddress> addresses = migrateBindAddresses(config);

        // parse the list of additional bind addresses
        Arrays
                .stream(StringUtils.split(StringUtils.defaultString(config.getBindAddresses()), ","))
                .map(s -> {
                    Matcher m = BIND_ADDRESSES_PATTERN.matcher(s);
                    if (!m.matches()) {
                        throw new IllegalArgumentException("bindAddresses: malformed: " + s);
                    }
                    return m;
                })
                .map(m -> new BindAddress(m.group("name"), URI.create(m.group("url"))))
                .forEach(addresses::add);

        // apply the filter
        if (schemes != null) {
            addresses.removeIf(a -> !schemes.contains(a.getAddress().getScheme()));
        }

        return addresses;
    }

    /**
     * Generates bind addresses based on legacy configuration properties.
     */
    private static List<BindAddress> migrateBindAddresses(ServiceConfiguration config) {
        List<BindAddress> addresses = new ArrayList<>(2);
        if (config.getBrokerServicePort().isPresent()) {
            addresses.add(new BindAddress(null, URI.create(
                    ServiceConfigurationUtils.brokerUrl(config.getBindAddress(), config.getBrokerServicePort().get()))));
        }
        if (config.getBrokerServicePortTls().isPresent()) {
            addresses.add(new BindAddress(null, URI.create(
                    ServiceConfigurationUtils.brokerUrlTls(config.getBindAddress(), config.getBrokerServicePortTls().get()))));
        }
        if (config.getWebServicePort().isPresent()) {
            addresses.add(new BindAddress(null, URI.create(
                    ServiceConfigurationUtils.webServiceUrl(config.getBindAddress(), config.getWebServicePort().get()))));
        }
        if (config.getWebServicePortTls().isPresent()) {
            addresses.add(new BindAddress(null, URI.create(
                    ServiceConfigurationUtils.webServiceUrlTls(config.getBindAddress(), config.getWebServicePortTls().get()))));
        }
        return addresses;
    }
}
