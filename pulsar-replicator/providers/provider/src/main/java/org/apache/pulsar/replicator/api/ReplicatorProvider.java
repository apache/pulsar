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
package org.apache.pulsar.replicator.api;

import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;

public interface ReplicatorProvider {
    /**
     * Returns Enum-type of replicator-provider so, replicator-service can map provider with its type
     * 
     * @return Enum-type of provider eg: Kinesis, KAFKA
     */
    public ReplicatorType getType();

    /**
     * Replicator-provider will require correct properties and credential to initialize replicator-producers so, this
     * method validates topic-properties and credential-properties.
     * 
     * @param namespace
     *            namespace of the topic to provide a reference to fetch authData from keystore
     * @param ReplicatorPolicies
     *            ReplicatorPolicies that contains replication metadata
     * 
     * @throws IllegalArgumentException
     */
    public void validateProperties(String namespace, ReplicatorPolicies replicatorPolicies)
            throws IllegalArgumentException;

    /**
     * 
     * Create Replicator-producer async which will publish messages to targeted replication-system.
     * 
     * @param topic
     * @param replicatorPolicies
     * @return
     */
    public CompletableFuture<ReplicatorProducer> createProducerAsync(final String topic,
            final ReplicatorPolicies replicatorPolicies);

}
