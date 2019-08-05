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
package org.apache.flink.batch.connectors.pulsar;

import org.apache.flink.batch.connectors.pulsar.serialization.JsonSerializationSchema;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;

/**
 * Pulsar Json Output Format to write Flink DataSets into a Pulsar topic in Json format.
 */
public class PulsarJsonOutputFormat<T> extends BasePulsarOutputFormat<T> {

    private static final long serialVersionUID = 8499620770848461958L;

    public PulsarJsonOutputFormat(String serviceUrl, String topicName, Authentication authentication) {
        super(serviceUrl, topicName, authentication);
        this.serializationSchema = new JsonSerializationSchema();
    }

    public PulsarJsonOutputFormat(ClientConfigurationData clientConfigurationData,
        ProducerConfigurationData producerConfigurationData) {
        super(clientConfigurationData, producerConfigurationData);
        this.serializationSchema = new JsonSerializationSchema();
    }
}
