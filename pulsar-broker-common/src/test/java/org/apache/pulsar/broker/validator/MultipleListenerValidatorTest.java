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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.net.InetAddress;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.Test;

/**
 * testcase for MultipleListenerValidator.
 */
public class MultipleListenerValidatorTest {

    // Deprecation warning suppressed as this test targets deprecated methods
    @SuppressWarnings("deprecation")
    @Test
    public void testGetAppliedAdvertised() throws Exception {
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setBrokerServicePort(Optional.of(6650));
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setAdvertisedListeners("internal:pulsar://192.0.0.1:6660, internal:pulsar+ssl://192.0.0.1:6651");
        config.setInternalListenerName("internal");
        assertEquals(ServiceConfigurationUtils.getAppliedAdvertisedAddress(config, false),
                "192.0.0.1");
        assertEquals(ServiceConfigurationUtils.getAppliedAdvertisedAddress(config, true),
                InetAddress.getLocalHost().getCanonicalHostName());

        config = newConfigWithNoPorts();
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setAdvertisedAddress("192.0.0.2");
        assertEquals(ServiceConfigurationUtils.getAppliedAdvertisedAddress(config, false),
                "192.0.0.2");
        assertEquals(ServiceConfigurationUtils.getAppliedAdvertisedAddress(config, true),
                "192.0.0.2");

        config.setAdvertisedAddress(null);
        assertEquals(ServiceConfigurationUtils.getAppliedAdvertisedAddress(config, false),
                ServiceConfigurationUtils.getDefaultOrConfiguredAddress(null));
        assertEquals(ServiceConfigurationUtils.getAppliedAdvertisedAddress(config, true),
                ServiceConfigurationUtils.getDefaultOrConfiguredAddress(null));
    }

    @Test
    public void testListenerDefaulting() {
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setInternalListenerName(null);
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651");
        MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        assertEquals("internal", config.getInternalListenerName());
    }

    @Test
    public void testBlankInternalListenerNameFallsBackToFirstAdvertisedListener() {
        // Backwards compatibility: if the user explicitly leaves `internalListenerName` blank but
        // configures `advertisedListeners` with custom names, the first listener parsed from
        // `advertisedListeners` is used as the internal listener — the previous Pulsar behavior.
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setInternalListenerName("");
        config.setAdvertisedListeners(
                "region1:pulsar://region1.example.com:6650,region2:pulsar://region2.example.com:6650");
        Map<String, AdvertisedListener> listeners =
                MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        // `region1` is the first declared, so it becomes the internal listener.
        assertEquals(config.getInternalListenerName(), "region1");
        // Both region entries are preserved in the result map.
        assertEquals(listeners.size(), 2);
        assertNotNull(listeners.get("region1"));
        assertNotNull(listeners.get("region2"));
    }

    @Test
    public void testBlankInternalListenerNameFallsBackToDefaultWhenNoAdvertisedListeners() {
        // When both `internalListenerName` and `advertisedListeners` are blank, the validator falls
        // back to the constant default `"internal"`.
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setInternalListenerName("");
        config.setBrokerServicePort(Optional.of(6650));
        config.setWebServicePort(Optional.of(8080));
        Map<String, AdvertisedListener> listeners =
                MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        assertEquals(config.getInternalListenerName(), ServiceConfiguration.DEFAULT_INTERNAL_LISTENER_NAME);
        assertNotNull(listeners.get(ServiceConfiguration.DEFAULT_INTERNAL_LISTENER_NAME),
                "the legacy-port-synthesized internal listener should be present");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMalformedListener() {
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setAdvertisedListeners(":pulsar://127.0.0.1:6660");
        MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerDuplicate_1() {
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651,"
                + " internal:pulsar://192.168.1.11:6660, internal:pulsar+ssl://192.168.1.11:6651");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerDuplicate_2() {
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660," + " internal:pulsar://192.168.1.11:6660");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerDuplicate_3() {
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setAdvertisedListeners(" internal:pulsar+ssl://127.0.0.1:6661,"
                + " internal:pulsar+ssl://192.168.1.11:6661");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDifferentListenerWithSameHostPort() {
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660," + " external:pulsar://127.0.0.1:6660");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
    }

    @Test
    public void testInternalListenerSynthesizedFromLegacyPorts() {
        // No advertisedListeners configured, only legacy ports. The internal listener is synthesized.
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setAdvertisedAddress("broker-1.example.com");
        config.setBrokerServicePort(Optional.of(6650));
        config.setWebServicePort(Optional.of(8080));
        Map<String, AdvertisedListener> listeners =
                MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        AdvertisedListener internal = listeners.get(ServiceConfiguration.DEFAULT_INTERNAL_LISTENER_NAME);
        assertNotNull(internal, "expected an `internal` listener to be synthesized from legacy ports");
        assertEquals(internal.getBrokerServiceUrl(), URI.create("pulsar://broker-1.example.com:6650"));
        assertEquals(internal.getBrokerHttpUrl(), URI.create("http://broker-1.example.com:8080"));
        assertNull(internal.getBrokerServiceUrlTls());
        assertNull(internal.getBrokerHttpsUrl());
    }

    @Test
    public void testInternalListenerSynthesizedWithAllPorts() {
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setAdvertisedAddress("broker-1.example.com");
        config.setBrokerServicePort(Optional.of(6650));
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setWebServicePort(Optional.of(8080));
        config.setWebServicePortTls(Optional.of(8443));
        Map<String, AdvertisedListener> listeners =
                MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        AdvertisedListener internal = listeners.get(ServiceConfiguration.DEFAULT_INTERNAL_LISTENER_NAME);
        assertNotNull(internal);
        assertEquals(internal.getBrokerServiceUrl(), URI.create("pulsar://broker-1.example.com:6650"));
        assertEquals(internal.getBrokerServiceUrlTls(), URI.create("pulsar+ssl://broker-1.example.com:6651"));
        assertEquals(internal.getBrokerHttpUrl(), URI.create("http://broker-1.example.com:8080"));
        assertEquals(internal.getBrokerHttpsUrl(), URI.create("https://broker-1.example.com:8443"));
    }

    @Test
    public void testInternalListenerAutoAddedWhenAdvertisedListenersDoesNotContainIt() {
        // User configures advertisedListeners with their own listener name but does not declare the
        // internal listener. The validator must auto-add it from the legacy ports.
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setAdvertisedAddress("broker-1.example.com");
        config.setBrokerServicePort(Optional.of(6650));
        config.setWebServicePort(Optional.of(8080));
        config.setAdvertisedListeners("region1:pulsar://region1.example.com:6660,"
                + "region1:pulsar+ssl://region1.example.com:6661");
        Map<String, AdvertisedListener> listeners =
                MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        assertEquals(listeners.size(), 2);
        AdvertisedListener region1 = listeners.get("region1");
        assertNotNull(region1);
        assertEquals(region1.getBrokerServiceUrl(), URI.create("pulsar://region1.example.com:6660"));
        AdvertisedListener internal = listeners.get("internal");
        assertNotNull(internal, "internal listener should have been auto-added from legacy ports");
        assertEquals(internal.getBrokerServiceUrl(), URI.create("pulsar://broker-1.example.com:6650"));
        assertEquals(internal.getBrokerHttpUrl(), URI.create("http://broker-1.example.com:8080"));
    }

    @Test
    public void testExplicitInternalListenerUrlsTakePrecedenceOverLegacyPorts() {
        // When the user explicitly declares URLs for the internal listener in advertisedListeners, those
        // URLs take precedence over the legacy-port-derived values. The legacy values still fill in any
        // URL slots the user did not declare.
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setAdvertisedAddress("broker-1.example.com");
        config.setBrokerServicePort(Optional.of(6650));
        config.setWebServicePort(Optional.of(8080));
        config.setAdvertisedListeners("internal:pulsar://explicit.example.com:6660,"
                + "internal:http://explicit.example.com:8888");
        Map<String, AdvertisedListener> listeners =
                MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        assertEquals(listeners.size(), 1);
        AdvertisedListener internal = listeners.get("internal");
        assertNotNull(internal);
        // Both URLs were declared explicitly: the user-provided values win over the legacy ports.
        assertEquals(internal.getBrokerServiceUrl(), URI.create("pulsar://explicit.example.com:6660"));
        assertEquals(internal.getBrokerHttpUrl(), URI.create("http://explicit.example.com:8888"));
        // The user did not declare TLS variants and the legacy TLS ports are unset.
        assertNull(internal.getBrokerServiceUrlTls());
        assertNull(internal.getBrokerHttpsUrl());
    }

    @Test
    public void testPartialInternalListenerOverrideMergesWithLegacyPorts() {
        // The user overrides only one URL (the binary endpoint) for the internal listener. The remaining
        // URLs must be filled in from the legacy-port configuration so the listener is complete.
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setAdvertisedAddress("broker-1.example.com");
        config.setBrokerServicePort(Optional.of(6650));
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setWebServicePort(Optional.of(8080));
        config.setWebServicePortTls(Optional.of(8443));
        config.setAdvertisedListeners("internal:pulsar://override.example.com:6660");
        Map<String, AdvertisedListener> listeners =
                MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        AdvertisedListener internal = listeners.get("internal");
        assertNotNull(internal);
        // Overridden by the explicit advertisedListeners entry:
        assertEquals(internal.getBrokerServiceUrl(), URI.create("pulsar://override.example.com:6660"));
        // Filled in from the legacy ports:
        assertEquals(internal.getBrokerServiceUrlTls(), URI.create("pulsar+ssl://broker-1.example.com:6651"));
        assertEquals(internal.getBrokerHttpUrl(), URI.create("http://broker-1.example.com:8080"));
        assertEquals(internal.getBrokerHttpsUrl(), URI.create("https://broker-1.example.com:8443"));
    }

    @Test
    public void testInternalListenerNameCustomizable() {
        // The internal listener name is configurable; the validator must look for the configured name.
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setInternalListenerName("region1");
        config.setAdvertisedAddress("broker-1.example.com");
        config.setBrokerServicePort(Optional.of(6650));
        config.setWebServicePort(Optional.of(8080));
        config.setAdvertisedListeners("region1:pulsar://region1.example.com:6660,"
                + "region1:http://region1.example.com:8888");
        Map<String, AdvertisedListener> listeners =
                MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        assertEquals(listeners.size(), 1);
        AdvertisedListener region1 = listeners.get("region1");
        assertNotNull(region1);
        assertEquals(region1.getBrokerServiceUrl(), URI.create("pulsar://region1.example.com:6660"));
    }

    @Test
    public void testFailureWhenNoInternalListenerAvailable() {
        // Custom internal listener name + no matching entry in advertisedListeners + no ports for the
        // legacy fallback => validation must fail with a clear message.
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setInternalListenerName("region1");
        config.setAdvertisedListeners("region2:pulsar://region2.example.com:6660");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config));
        assertTrue(e.getMessage().contains("region1"), "expected error to mention the listener name: "
                + e.getMessage());
    }

    @Test
    public void testInternalListenerNameDefaultsToInternalConstant() {
        ServiceConfiguration config = new ServiceConfiguration();
        assertEquals(config.getInternalListenerName(), ServiceConfiguration.DEFAULT_INTERNAL_LISTENER_NAME);
    }

    @Test
    public void testWhitespaceBetweenEntriesIsTrimmed() {
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setInternalListenerName("internal");
        // Spaces, newlines, and tabs around the comma separators and around the entries themselves
        // must all be tolerated and trimmed.
        config.setAdvertisedListeners(
                "  internal:pulsar://broker-1:6650 ,\n\texternal:pulsar://broker-1.public:6650  ");
        Map<String, AdvertisedListener> listeners =
                MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        assertEquals(listeners.size(), 2);
        assertEquals(listeners.get("internal").getBrokerServiceUrl(), URI.create("pulsar://broker-1:6650"));
        assertEquals(listeners.get("external").getBrokerServiceUrl(),
                URI.create("pulsar://broker-1.public:6650"));
    }

    @Test
    public void testEmptyEntriesAreSkipped() {
        ServiceConfiguration config = newConfigWithNoPorts();
        config.setInternalListenerName("internal");
        // A trailing comma or an extra comma in the middle should be ignored.
        config.setAdvertisedListeners("internal:pulsar://broker-1:6650, ,external:pulsar://broker-1.public:6650,");
        Map<String, AdvertisedListener> listeners =
                MultipleListenerValidator.validateAndUpdateAdvertisedListeners(config);
        assertEquals(listeners.size(), 2);
        assertEquals(listeners.get("internal").getBrokerServiceUrl(), URI.create("pulsar://broker-1:6650"));
        assertEquals(listeners.get("external").getBrokerServiceUrl(),
                URI.create("pulsar://broker-1.public:6650"));
    }

    private ServiceConfiguration newConfigWithNoPorts() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePort(Optional.empty());
        config.setBrokerServicePortTls(Optional.empty());
        config.setWebServicePort(Optional.empty());
        config.setWebServicePortTls(Optional.empty());
        return config;
    }
}
