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
package org.apache.pulsar.client.impl.v5.auth;

import static org.assertj.core.api.Assertions.assertThat;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.testng.annotations.Test;

/**
 * Verifies the built-in mTLS plugin: it advertises the {@code "tls"} method name and carries an empty
 * binary payload, since the certificate is presented at the TLS handshake.
 */
public class TlsAuthenticationTest {

    @Test
    public void defaultMethodNameIsTls() throws Exception {
        TlsAuthentication auth = new TlsAuthentication();
        assertThat(auth.authMethodName()).isEqualTo("tls");
        auth.initializeAsync(null).get();

        BinaryAuthData data = auth.capability(BinaryAuthDataProvider.class).orElseThrow()
                .getAuthDataAsync(new SimpleAuthCallContext("broker")).get();
        assertThat(data.bytes()).isEmpty();
    }
}
