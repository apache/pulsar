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
package org.apache.pulsar.broker.web;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.net.InetAddress;
import java.net.URI;
import java.util.Optional;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies behavior when the internal listener has more than one bind address for the same protocol
 * scheme. In that case the broker accepts every binding but uses the first declared one (per scheme)
 * as the primary, which is what drives {@code pulsar.getBrokerServiceUrl()},
 * {@code pulsar.getWebServiceAddress()}, and the synthesized internal advertised listener.
 */
@Test(groups = "broker")
public class MultipleInternalBindAddressesTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // Drive everything off `bindAddresses` so the validator/binder cannot reach back to legacy
        // ports to find a "default" primary.
        conf.setBrokerServicePort(Optional.empty());
        conf.setBrokerServicePortTls(Optional.empty());
        conf.setWebServicePort(Optional.empty());
        conf.setWebServicePortTls(Optional.empty());
        // Two pulsar bindings and two http bindings for the internal listener, on different IPs so
        // the ip:port uniqueness check is satisfied (each combination is unique). Both use port 0
        // so the OS assigns distinct dynamic ports.
        conf.setBindAddresses(
                "internal:pulsar://0.0.0.0:0,"
                        + "internal:pulsar://127.0.0.1:0,"
                        + "internal:http://0.0.0.0:0,"
                        + "internal:http://127.0.0.1:0");
        // Hostname-fallback path for the advertised address.
        conf.setAdvertisedAddress(null);
        conf.setAdvertisedListeners(null);
    }

    @Test
    public void testFirstInternalBindingPerSchemeBecomesPrimary() throws Exception {
        ServiceConfiguration runtimeConf = pulsar.getConfiguration();

        // The primary listen ports are surfaced via the dedicated methods. By contract, these come
        // from the FIRST binding (per scheme) that matches the internal listener.
        int primaryPulsarPort = pulsar.getBrokerListenPort().orElseThrow();
        int primaryWebPort = pulsar.getWebService().getListenPortHTTP().orElseThrow();
        assertTrue(primaryPulsarPort > 0, "primary pulsar port must be a real OS-assigned port");
        assertTrue(primaryWebPort > 0, "primary web port must be a real OS-assigned port");
        assertTrue(primaryPulsarPort != primaryWebPort,
                "pulsar and web primaries must have been assigned distinct ports");

        // After binding, the configuration's legacy ports must reflect the primary binding's ports
        // (not the additional bindings' ports), which is what downstream synthesis depends on.
        assertEquals(runtimeConf.getBrokerServicePort(), Optional.of(primaryPulsarPort));
        assertEquals(runtimeConf.getWebServicePort(), Optional.of(primaryWebPort));

        // The bindAddresses config string still records all the original entries the user declared,
        // proving the multi-binding configuration was accepted up-front.
        String declared = runtimeConf.getBindAddresses();
        assertNotNull(declared);
        assertEquals(countOccurrences(declared, "internal:pulsar://"), 2,
                "two internal:pulsar:// bindings should be in the declared config: " + declared);
        assertEquals(countOccurrences(declared, "internal:http://"), 2,
                "two internal:http:// bindings should be in the declared config: " + declared);

        // pulsar.getBrokerServiceUrl() / pulsar.getWebServiceAddress() must use the primary ports
        // (not any of the additional bindings' ports).
        String expectedHost = InetAddress.getLocalHost().getCanonicalHostName();
        assertEquals(pulsar.getBrokerServiceUrl(), "pulsar://" + expectedHost + ":" + primaryPulsarPort);
        assertEquals(pulsar.getWebServiceAddress(), "http://" + expectedHost + ":" + primaryWebPort);

        // The synthesized internal advertised listener must agree with the same primary ports.
        AdvertisedListener internal =
                pulsar.getAdvertisedListeners().get(ServiceConfiguration.DEFAULT_INTERNAL_LISTENER_NAME);
        assertNotNull(internal,
                "the `internal` listener must have been synthesized after binding, but the map was: "
                        + pulsar.getAdvertisedListeners());
        assertEquals(internal.getBrokerServiceUrl(),
                URI.create("pulsar://" + expectedHost + ":" + primaryPulsarPort));
        assertEquals(internal.getBrokerHttpUrl(),
                URI.create("http://" + expectedHost + ":" + primaryWebPort));
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int index = 0;
        while ((index = haystack.indexOf(needle, index)) != -1) {
            count++;
            index += needle.length();
        }
        return count;
    }
}
