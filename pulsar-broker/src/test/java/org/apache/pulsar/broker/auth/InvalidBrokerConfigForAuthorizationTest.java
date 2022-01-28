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
package org.apache.pulsar.broker.auth;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import org.apache.pulsar.broker.PulsarServerException;
import org.testng.annotations.Test;

public class InvalidBrokerConfigForAuthorizationTest extends MockedPulsarServiceBaseTest {

    @Test
    void startupShouldFailWhenAuthorizationIsEnabledWithoutAuthentication() throws Exception {
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationEnabled(false);
        try {
            internalSetup();
            fail("An exception should have been thrown");
        } catch (Exception e) {
            assertEquals(e.getClass(), PulsarServerException.class);
            assertEquals(e.getCause().getClass(), IllegalStateException.class);
            assertEquals(e.getCause().getMessage(), "Invalid broker configuration. Authentication must be "
                    + "enabled with authenticationEnabled=true when authorization is enabled with "
                    + "authorizationEnabled=true.");
        }
    }

    @Override
    protected void setup() throws Exception {

    }

    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }
}
