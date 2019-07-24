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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.Test;

public class ClientInitializationTest {

    @Test
    public void testInitializeAuthWithTls() throws PulsarClientException {
        Authentication auth = mock(Authentication.class);

        PulsarClient.builder()
                .serviceUrl("pulsar+ssl://my-host:6650")
                .authentication(auth)
                .build();

        // Auth should only be started, though we shouldn't have tried to get credentials yet (until we first attempt to
        // connect).
        verify(auth).start();
        verifyNoMoreInteractions(auth);
    }
}
