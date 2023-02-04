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
package org.apache.pulsar.client.admin.internal;

import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link PulsarAdminImpl}.
 */
public class PulsarAdminImplTest {

    @Test
    public void testAuthDisabledWhenAuthNotSpecifiedAnywhere() {
        assertThat(createAdminAndGetAuth(new ClientConfigurationData()))
                .isInstanceOf(AuthenticationDisabled.class);
    }

    @Test
    public void testAuthFromConfUsedWhenConfHasAuth() {
        Authentication auth = mock(Authentication.class);
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setAuthentication(auth);
        assertThat(createAdminAndGetAuth(conf)).isSameAs(auth);
    }

    @SneakyThrows
    private Authentication createAdminAndGetAuth(ClientConfigurationData conf) {
        try (PulsarAdminImpl admin = new PulsarAdminImpl("http://localhost:8080", conf)) {
            return admin.auth;
        }
    }
}
