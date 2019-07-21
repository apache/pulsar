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

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.testng.annotations.Test;

public class ClientBuilderImplTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithServiceUrlAndServiceUrlProviderNotSet() throws PulsarClientException {
        PulsarClient.builder().build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithNullServiceUrl() throws PulsarClientException {
        PulsarClient.builder().serviceUrl(null).build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithNullServiceUrlProvider() throws PulsarClientException {
        PulsarClient.builder().serviceUrlProvider(null).build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithServiceUrlAndServiceUrlProvider() throws PulsarClientException {
        PulsarClient.builder().serviceUrlProvider(new ServiceUrlProvider() {
            @Override
            public void initialize(PulsarClient client) {

            }

            @Override
            public String getServiceUrl() {
                return "pulsar://localhost:6650";
            }
        }).serviceUrl("pulsar://localhost:6650").build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithBlankServiceUrlInServiceUrlProvider() throws PulsarClientException {
        PulsarClient.builder().serviceUrlProvider(new ServiceUrlProvider() {
            @Override
            public void initialize(PulsarClient client) {

            }

            @Override
            public String getServiceUrl() {
                return "";
            }
        }).build();
    }

}
