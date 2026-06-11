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
package org.apache.pulsar.client.impl.auth.oauth2;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockConstruction;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.mockito.MockedConstruction;

final class OAuth2MockHttpClient {

    private OAuth2MockHttpClient() {
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    static void withMockedSslFactory(ThrowingRunnable runnable) throws Exception {
        try (MockedConstruction<DefaultPulsarSslFactory> ignoredSslFactory =
                     mockConstruction(DefaultPulsarSslFactory.class, (mock, context) -> {
                         doNothing().when(mock).initialize(any());
                         doNothing().when(mock).createInternalSslContext();
                     });
             MockedConstruction<DefaultAsyncHttpClient> ignoredHttpClient =
                     mockConstruction(DefaultAsyncHttpClient.class)) {
            runnable.run();
        }
    }
}
