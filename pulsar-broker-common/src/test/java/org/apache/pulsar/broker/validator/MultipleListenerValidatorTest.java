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

import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

import java.util.Optional;

/**
 * testcase for MultipleListenerValidator.
 */
public class MultipleListenerValidatorTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAppearTogether() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedAddress("127.0.0.1");
        config.setAdvertisedListeners("internal:pulsar://192.168.1.11:6660,internal:pulsar+ssl://192.168.1.11:6651");
        config.setInternalListenerName("internal");
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
    public void testDifferentListenerWithSameHostPort() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660," + " external:pulsar://127.0.0.1:6660");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerWithoutTLSPort() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test
    public void testListenerWithTLSPort() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testListenerWithoutNonTLSAddress() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setAdvertisedListeners(" internal:pulsar+ssl://127.0.0.1:6651");
        config.setInternalListenerName("internal");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testWithoutListenerNameInAdvertisedListeners() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerServicePortTls(Optional.of(6651));
        config.setAdvertisedListeners(" internal:pulsar://127.0.0.1:6660, internal:pulsar+ssl://127.0.0.1:6651");
        config.setInternalListenerName("external");
        MultipleListenerValidator.validateAndAnalysisAdvertisedListener(config);
    }

}
