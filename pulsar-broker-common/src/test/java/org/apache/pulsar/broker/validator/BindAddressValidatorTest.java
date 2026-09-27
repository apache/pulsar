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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.BindAddress;
import org.testng.annotations.Test;

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
        assertEquals(addresses, Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("internal", URI.create("pulsar+ssl://0.0.0.0:6651"))));
    }

    @Test
    public void testMultiListener() {
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBindAddresses("internal:pulsar://0.0.0.0:6650,external:pulsar+ssl://0.0.0.0:6651");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(addresses, Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("external", URI.create("pulsar+ssl://0.0.0.0:6651"))));
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
        assertEquals(addresses, Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("internal", URI.create("pulsar+ssl://0.0.0.0:6651")),
                new BindAddress("internal", URI.create("http://0.0.0.0:8080")),
                new BindAddress("internal", URI.create("https://0.0.0.0:443"))));
    }

    @Test
    public void testMigrationWithDefaults() {
        ServiceConfiguration config = new ServiceConfiguration();
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(addresses, Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("internal", URI.create("http://0.0.0.0:8080"))));
    }

    @Test
    public void testMigrationWithExtra() {
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBrokerServicePort(Optional.of(6650));
        config.setBindAddresses("extra:pulsar://0.0.0.0:6652");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(addresses, Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("extra", URI.create("pulsar://0.0.0.0:6652"))));
    }

    @Test
    public void testSchemeFilter() {
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBrokerServicePort(Optional.of(6650));
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setBindAddresses("extra:pulsar://0.0.0.0:6652,extra:http://0.0.0.0:8080");

        List<BindAddress> addresses;
        addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(addresses, Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("internal", URI.create("pulsar+ssl://0.0.0.0:6651")),
                new BindAddress("extra", URI.create("pulsar://0.0.0.0:6652")),
                new BindAddress("extra", URI.create("http://0.0.0.0:8080"))));

        addresses = BindAddressValidator.validateBindAddresses(config, Arrays.asList("pulsar", "pulsar+ssl"));
        assertEquals(addresses, Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("internal", URI.create("pulsar+ssl://0.0.0.0:6651")),
                new BindAddress("extra", URI.create("pulsar://0.0.0.0:6652"))));

        addresses = BindAddressValidator.validateBindAddresses(config, Collections.singletonList("http"));
        assertEquals(addresses, Collections.singletonList(
                new BindAddress("extra", URI.create("http://0.0.0.0:8080"))));
    }

    @Test
    public void testMigrationUsesConfiguredInternalListenerName() {
        ServiceConfiguration config = newEmptyConfiguration();
        config.setInternalListenerName("region1");
        config.setBrokerServicePort(Optional.of(6650));
        config.setWebServicePort(Optional.of(8080));
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(addresses, Arrays.asList(
                new BindAddress("region1", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("region1", URI.create("http://0.0.0.0:8080"))));
    }

    @Test
    public void testDuplicateMatchingMigratedBindingIsTolerated() {
        // User explicitly re-declares the migrated binding under the same listener name; this must
        // be accepted without throwing.
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBrokerServicePort(Optional.of(6650));
        config.setBindAddresses("internal:pulsar://0.0.0.0:6650");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(addresses, Collections.singletonList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650"))));
    }

    @Test
    public void testDuplicateConflictFailsWithError() {
        // The migrated binding uses the internal listener name, but the user re-declares the same
        // URI under a different listener name. This is a conflict and must be rejected.
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBrokerServicePort(Optional.of(6650));
        config.setBindAddresses("external:pulsar://0.0.0.0:6650");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> BindAddressValidator.validateBindAddresses(config, null));
        assertTrue(e.getMessage().contains("conflicting listener names"),
                "expected conflict message but got: " + e.getMessage());
    }

    @Test
    public void testDuplicateBindAddressesEntriesAreTolerated() {
        // Two identical entries in bindAddresses should not cause a failure.
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBindAddresses("extra:pulsar://0.0.0.0:6652,extra:pulsar://0.0.0.0:6652");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(addresses, Collections.singletonList(
                new BindAddress("extra", URI.create("pulsar://0.0.0.0:6652"))));
    }

    @Test
    public void testSameIpPortDifferentSchemeSameListenerFails() {
        // Same ip:port with two different schemes (even for the same listener) cannot both be bound.
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBindAddresses("internal:pulsar://0.0.0.0:8080,internal:http://0.0.0.0:8080");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> BindAddressValidator.validateBindAddresses(config, null));
        assertTrue(e.getMessage().contains("0.0.0.0:8080"),
                "expected ip:port in message but got: " + e.getMessage());
        assertTrue(e.getMessage().contains("only one scheme"),
                "expected scheme-collision wording but got: " + e.getMessage());
    }

    @Test
    public void testSameIpPortDifferentSchemeDifferentListenerFails() {
        // Same ip:port with two different schemes and two different listener names also fails for
        // the same reason: an ip:port can only be bound by one (listener, scheme).
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBindAddresses("internal:pulsar://0.0.0.0:8080,external:http://0.0.0.0:8080");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> BindAddressValidator.validateBindAddresses(config, null));
        assertTrue(e.getMessage().contains("0.0.0.0:8080"),
                "expected ip:port in message but got: " + e.getMessage());
    }

    @Test
    public void testDynamicPortZeroIsSkippedFromIpPortUniqueness() {
        // Port 0 means "OS-assigned ephemeral port", so two port-0 bindings with the same IP cannot
        // actually collide — each socket gets a different kernel-assigned port. The ip:port
        // uniqueness check therefore skips port-0 entries.
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBindAddresses("internal:pulsar://0.0.0.0:0,internal:http://0.0.0.0:0");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(addresses, Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:0")),
                new BindAddress("internal", URI.create("http://0.0.0.0:0"))));
    }

    @Test
    public void testLegacyMigrationIpPortCollisionFails() {
        // brokerServicePort and webServicePort collide on the same ip:port: the migrated bindings
        // would produce pulsar://0.0.0.0:8080 and http://0.0.0.0:8080, which cannot coexist.
        ServiceConfiguration config = newEmptyConfiguration();
        config.setBrokerServicePort(Optional.of(8080));
        config.setWebServicePort(Optional.of(8080));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> BindAddressValidator.validateBindAddresses(config, null));
        assertTrue(e.getMessage().contains("0.0.0.0:8080"),
                "expected ip:port in message but got: " + e.getMessage());
    }

    @Test
    public void testWhitespaceBetweenEntriesIsTrimmed() {
        ServiceConfiguration config = newEmptyConfiguration();
        // Spaces, newlines, and tabs around the comma separators and around the entries themselves
        // must all be tolerated and trimmed.
        config.setBindAddresses("  internal:pulsar://0.0.0.0:6650 ,\n\texternal:pulsar+ssl://0.0.0.0:6651  ");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(addresses, Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("external", URI.create("pulsar+ssl://0.0.0.0:6651"))));
    }

    @Test
    public void testEmptyEntriesAreSkipped() {
        ServiceConfiguration config = newEmptyConfiguration();
        // A trailing comma or an extra comma in the middle should be ignored, not flagged as malformed.
        config.setBindAddresses("internal:pulsar://0.0.0.0:6650, ,external:pulsar+ssl://0.0.0.0:6651,");
        List<BindAddress> addresses = BindAddressValidator.validateBindAddresses(config, null);
        assertEquals(addresses, Arrays.asList(
                new BindAddress("internal", URI.create("pulsar://0.0.0.0:6650")),
                new BindAddress("external", URI.create("pulsar+ssl://0.0.0.0:6651"))));
    }
}
