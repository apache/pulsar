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
package org.apache.pulsar.connect.util;

import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.connect.config.ConnectorConfiguration;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class PulsarUtils {

    public static PulsarClient createClient(Properties properties) throws PulsarClientException {
        final ClientConfiguration configuration = new ClientConfiguration();
        configuration.setOperationTimeout(
                ConnectorConfiguration.DEFAULT_OPERATION_TIMEOUT_SECONDS,
                TimeUnit.SECONDS);
        final String serviceUrl =
                properties.getProperty(ConnectorConfiguration.KEY_SERVICE_URL,
                        ConnectorConfiguration.DEFAULT_SERVICE_URL);

        return PulsarClient.create(serviceUrl, configuration);
    }

    private PulsarUtils() {}
}
