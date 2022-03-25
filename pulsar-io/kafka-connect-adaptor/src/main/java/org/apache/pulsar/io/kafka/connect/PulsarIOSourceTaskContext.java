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
package org.apache.pulsar.io.kafka.connect;

import java.util.Map;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

class PulsarIOSourceTaskContext implements SourceTaskContext {

    private final OffsetStorageReader reader;
    private final PulsarKafkaWorkerConfig pulsarKafkaWorkerConfig;

    PulsarIOSourceTaskContext(OffsetStorageReader reader, PulsarKafkaWorkerConfig pulsarKafkaWorkerConfig) {
        this.reader = reader;
        this.pulsarKafkaWorkerConfig = pulsarKafkaWorkerConfig;
    }

    @Override
    public Map<String, String> configs() {
        return pulsarKafkaWorkerConfig.originalsStrings();
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return reader;
    }
}
