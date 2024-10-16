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
package org.apache.pulsar.broker.authentication.oidc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.fail;
import javax.naming.AuthenticationException;
import org.testng.annotations.Test;

/**
 * Unit tests primarily focused on failure scenarios for {@link AuthenticationStateOpenID}. The happy path is covered
 * by {@link AuthenticationProviderOpenIDIntegrationTest}.
 */
public class AuthenticationStateOpenIDTest {
    @Test
    void getRoleOnAuthStateShouldFailIfNotAuthenticated() {
        AuthenticationStateOpenID state = new AuthenticationStateOpenID(null, null, null);
        assertFalse(state.isComplete());
        assertThrows(AuthenticationException.class, state::getAuthRole);
    }

    @Test
    void getAuthDataOnAuthStateShouldBeNullIfNotAuthenticated() {
        AuthenticationStateOpenID state = new AuthenticationStateOpenID(null, null, null);
        assertNull(state.getAuthDataSource());
    }

    // We override this behavior to make it clear that this provider is only meant to be used asynchronously.
    @SuppressWarnings("deprecation")
    @Test
    void authenticateShouldThrowNotImplementedException() {
        AuthenticationStateOpenID state = new AuthenticationStateOpenID(null, null, null);
        try {
            state.authenticate(null);
            fail("Expected AuthenticationException to be thrown");
        } catch (AuthenticationException e) {
            assertEquals(e.getMessage(), "Not supported");
        }
    }
}
