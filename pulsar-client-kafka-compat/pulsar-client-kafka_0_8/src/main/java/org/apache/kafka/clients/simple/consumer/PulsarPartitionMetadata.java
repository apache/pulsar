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
package org.apache.kafka.clients.simple.consumer;

import java.util.Collections;
import java.util.List;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;

public class PulsarPartitionMetadata extends PartitionMetadata {

    private final List<Broker> hosts;

    public PulsarPartitionMetadata(String hostUrl, int port) {
        super(null);
        this.hosts = Collections.singletonList(new Broker(0, hostUrl, port));
    }

    @Override
    public List<Broker> replicas() {
        return hosts;
    }

    @Override
    public Broker leader() {
        return hosts.get(0);
    }

    @Override
    public int partitionId() {
        // it always returns partition=-1
        return -1;
    }
}
