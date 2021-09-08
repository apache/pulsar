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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.common.configuration.BindAddress;

import static java.util.stream.Collectors.toList;

/**
 * Validates bind address configurations.
 */
public class BindAddressValidator {

    private static final Pattern bindAddress = Pattern.compile("(?<name>\\w+):(?<url>.+)$");

    /**
     * Validate the configuration of `bindAddresses`.
     * @param config the pulsar broker configure.
     * @return a list of bind addresses.
     */
    public static List<BindAddress> validateBindAddresses(ServiceConfiguration config) {
        // migrate the existing configuration properties
        List<BindAddress> addresses = migrateBindAddresses(config);
        if (StringUtils.isBlank(config.getBindAddresses())) {
            return addresses;
        }

        // parse the list of additional bind addresses
        Arrays
                .stream(StringUtils.split(config.getBindAddresses(), ","))
                .map(bindAddress::matcher)
                .filter(Matcher::matches)
                .map(m -> new BindAddress(m.group("name"), URI.create(m.group("url"))))
                .filter(m -> StringUtils.equalsAnyIgnoreCase(m.getAddress().getScheme(), "pulsar", "pulsar+ssl"))
                .forEach(addresses::add);

        return addresses;
    }

    /**
     * Generates bind addresses based on legacy configuration.
     * When no bind addresses are defined:
     * - generate a bind address for the internal listener
     *   on bindAddress:brokerServicePort and/or bindAddress:brokerServicePortTls.
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
        return addresses;
    }
}
