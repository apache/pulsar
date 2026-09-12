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
package org.apache.pulsar.functions.worker;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import java.util.Set;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.authorization.MultiRolesTokenAuthorizationProvider;
import org.testng.annotations.Test;

public class WorkerTest {
    @Test
    public void testStandaloneMultiRoleAuthorization() throws Exception {
        SecretKey key = KeyGenerator.getInstance("HmacSHA256").generateKey();
        WorkerConfig config = new WorkerConfig();
        config.setAuthenticationEnabled(true);
        config.setAuthorizationEnabled(true);
        config.setAuthenticationProviders(Set.of(AuthenticationProviderToken.class.getName()));
        config.setAuthorizationProvider(MultiRolesTokenAuthorizationProvider.class.getName());
        config.setSuperUserRoles(Set.of("admin"));
        config.setConfigurationMetadataStoreUrl("memory:worker-multi-role");
        config.getProperties().setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(key));
        config.getProperties().setProperty("tokenAuthClaim", "sub");
        Worker worker = new Worker(config);
        try {
            AuthorizationService authorization = worker.initializeAuthorizationService();
            String token = AuthTokenUtils.createToken(key, "admin", Optional.empty());
            assertThat(authorization.isSuperUser("user", new AuthenticationDataCommand(token)).get()).isTrue();
            String otherToken = AuthTokenUtils.createToken(key, "user", Optional.empty());
            assertThat(authorization.isSuperUser("user", new AuthenticationDataCommand(otherToken)).get()).isFalse();
        } finally {
            worker.stop();
        }
    }
}
