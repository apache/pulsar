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

import java.net.InetAddress;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.AssertJUnit.assertEquals;

/**
 * testcase for MultipleListenerValidator.
 */
public class MultipleListenerValidatorTest {

    @Test
    public void testGetAppliedAdvertised() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setAdvertisedListeners("internal:pulsar://192.0.0.1:6660, internal:pulsar+ssl://192.0.0.1:6651");
        config.setInternalListenerName("internal");
        assertEquals(ServiceConfigurationUtils.getAppliedAdvertisedAddress(config, false),
                "192.0.0.1");
        assertEquals(ServiceConfigurationUtils.getAppliedAdvertisedAddress(config, true),
                InetAddress.getLocalHost().getCanonicalHostName());

        config = new ServiceConfiguration();
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
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
        assertEquals("internal", config.getInternalListenerName());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMalformedListener() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(":pulsar://127.0.0.1:6660");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerDuplicate_1() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651,"
                + " internal:pulsar://192.168.1.11:6660, internal:pulsar+ssl://192.168.1.11:6651");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerDuplicate_2() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660," + " internal:pulsar://192.168.1.11:6660");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerDuplicate_3() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar+ssl://127.0.0.1:6661," + " internal:pulsar+ssl://192.168.1.11:6661");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDifferentListenerWithSameHostPort() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660," + " external:pulsar://127.0.0.1:6660");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testWithoutListenerNameInAdvertisedListeners() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651");
        config.setInternalListenerName("external");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

}
