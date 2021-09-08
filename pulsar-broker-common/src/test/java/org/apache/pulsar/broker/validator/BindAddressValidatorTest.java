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

    @Test
    public void testInvalidScheme() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.empty());
        config.setBindAddresses("internal:pulsar://0.0.0.0:6650,internal:invalid://0.0.0.0:6651");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config);
        assertEquals(1, addresses.size());
        assertEquals(new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")), addresses.get(0));
    }

    @Test
    public void testMalformed() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.empty());
        config.setBindAddresses("internal:");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config);
        assertEquals(0, addresses.size());
    }

    @Test
    public void testOneListenerMultipleAddresses() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.empty());
        config.setBrokerServicePortTls(Optional.empty());
        config.setBindAddresses("internal:pulsar://0.0.0.0:6650,internal:pulsar+ssl://0.0.0.0:6651");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config);
        assertEquals(2, addresses.size());
        assertEquals(new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")), addresses.get(0));
        assertEquals(new BindAddress("internal", URI.create("pulsar+ssl://0.0.0.0:6651")), addresses.get(1));
    }

    @Test
    public void testMultiListener() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.empty());
        config.setBrokerServicePortTls(Optional.empty());
        config.setBindAddresses("internal:pulsar://0.0.0.0:6650,external:pulsar+ssl://0.0.0.0:6651");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config);
        assertEquals(2, addresses.size());
        assertEquals(new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")), addresses.get(0));
        assertEquals(new BindAddress("external", URI.create("pulsar+ssl://0.0.0.0:6651")), addresses.get(1));
    }

    @Test
    public void testMigrationNonTls() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.of(6650));
        config.setBrokerServicePortTls(Optional.empty());
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config);
        assertEquals(1, addresses.size());
        assertEquals(new BindAddress(null, URI.create("pulsar://0.0.0.0:6650")), addresses.get(0));
    }

    @Test
    public void testMigrationNonTlsWithExtra() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.of(6650));
        config.setBrokerServicePortTls(Optional.empty());
        config.setBindAddresses("extra:pulsar://0.0.0.0:6652");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config);
        assertEquals(2, addresses.size());
        assertEquals(new BindAddress(null, URI.create("pulsar://0.0.0.0:6650")), addresses.get(0));
        assertEquals(new BindAddress("extra", URI.create("pulsar://0.0.0.0:6652")), addresses.get(1));
    }

    @Test
    public void testMigrationTls() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.empty());
        config.setBrokerServicePortTls(Optional.of(6651));
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config);
        assertEquals(1, addresses.size());
        assertEquals(new BindAddress(null, URI.create("pulsar+ssl://0.0.0.0:6651")), addresses.get(0));
    }

    @Test
    public void testMigrationTlsWithExtra() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.empty());
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setBindAddresses("extra:pulsar://0.0.0.0:6652");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config);
        assertEquals(2, addresses.size());
        assertEquals(new BindAddress(null, URI.create("pulsar+ssl://0.0.0.0:6651")), addresses.get(0));
        assertEquals(new BindAddress("extra", URI.create("pulsar://0.0.0.0:6652")), addresses.get(1));
    }
}
