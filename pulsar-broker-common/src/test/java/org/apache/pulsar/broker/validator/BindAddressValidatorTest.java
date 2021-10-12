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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.BindAddress;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * testcase for BindAddressValidator.
 */
public class BindAddressValidatorTest {

    /**
     * Provides a configuration with no bind addresses specified.
     */
    private ServiceConfiguration newEmptyConfiguration() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.empty());  // default: 6650
        config.setBrokerServicePortTls(Optional.empty());
        config.setWebServicePort(Optional.empty());     // default: 8080
        config.setWebServicePortTls(Optional.empty());
        return config;
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMalformed() {
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBindAddresses("internal:");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(0, addresses.size());
    }

    @Test
    public void testOneListenerMultipleAddresses() {
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBindAddresses("internal:pulsar://0.0.0.0:6650,internal:pulsar+ssl://0.0.0.0:6651");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("internal", URI.create("pulsar+ssl://0.0.0.0:6651"))), addresses);
    }

    @Test
    public void testMultiListener() {
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBindAddresses("internal:pulsar://0.0.0.0:6650,external:pulsar+ssl://0.0.0.0:6651");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("external", URI.create("pulsar+ssl://0.0.0.0:6651"))), addresses);
    }

    @Test
    public void testMigrationWithAllOptions() {
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBrokerServicePort(Optional.of(6650));
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setWebServicePort(Optional.of(8080));
        config.setWebServicePortTls(Optional.of(443));
        config.setBindAddress("0.0.0.0");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(Arrays.asList(
                new BindAddress(null, URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress(null, URI.create("pulsar+ssl://0.0.0.0:6651")),
                new BindAddress(null, URI.create("http://0.0.0.0:8080")),
                new BindAddress(null, URI.create("https://0.0.0.0:443"))), addresses);
    }

    @Test
    public void testMigrationWithDefaults() {
        ServiceConfiguration config = new ServiceConfiguration();
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(Arrays.asList(
                new BindAddress(null, URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress(null, URI.create("http://0.0.0.0:8080"))), addresses);
    }

    @Test
    public void testMigrationWithExtra() {
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBrokerServicePort(Optional.of(6650));
        config.setBindAddresses("extra:pulsar://0.0.0.0:6652");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(Arrays.asList(
                new BindAddress(null, URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("extra", URI.create("pulsar://0.0.0.0:6652"))), addresses);
    }

    @Test
    public void testSchemeFilter() {
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBrokerServicePort(Optional.of(6650));
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setBindAddresses("extra:pulsar://0.0.0.0:6652,extra:http://0.0.0.0:8080");

        List<BindAddress> addresses;
        addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(Arrays.asList(
                new BindAddress(null, URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress(null, URI.create("pulsar+ssl://0.0.0.0:6651")),
                new BindAddress("extra", URI.create("pulsar://0.0.0.0:6652")),
                new BindAddress("extra", URI.create("http://0.0.0.0:8080"))), addresses);

        addresses = BindAddressValidator.validateBindAddresses(config, Arrays.asList("pulsar", "pulsar+ssl"));
        assertEquals(Arrays.asList(
                new BindAddress(null, URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress(null, URI.create("pulsar+ssl://0.0.0.0:6651")),
                new BindAddress("extra", URI.create("pulsar://0.0.0.0:6652"))), addresses);

        addresses = BindAddressValidator.validateBindAddresses(config, Collections.singletonList("http"));
        assertEquals(Collections.singletonList(
                new BindAddress("extra", URI.create("http://0.0.0.0:8080"))), addresses);
    }
}
