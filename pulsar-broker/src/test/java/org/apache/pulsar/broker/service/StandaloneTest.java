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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.PulsarStandaloneStarter;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

@Test(groups = "broker")
public class StandaloneTest extends MockedPulsarServiceBaseTest {

    @Override
    protected void setup() throws Exception {

    }

    @Override
    protected void cleanup() throws Exception {

    }

    @Test
    public void testAdvertised() throws Exception {
        String args[] = new String[]{"--config",
                "./src/test/resources/configurations/pulsar_broker_test_standalone.conf"};
        PulsarStandaloneStarter standalone = new PulsarStandaloneStarter(args);
        assertNull(standalone.getConfig().getAdvertisedAddress());
        assertEquals(standalone.getConfig().getAdvertisedListeners(),
                "internal:pulsar://192.168.1.11:6660,internal:pulsar+ssl://192.168.1.11:6651");
    }
}
