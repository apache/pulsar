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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.runtime.WorkerConfig;

/**
 * Pulsar Kafka Worker Config.
 */
public class PulsarKafkaWorkerConfig extends WorkerConfig {

    private static final ConfigDef CONFIG;

    /**
     * <code>offset.storage.topic</code>
     */
    public static final String OFFSET_STORAGE_TOPIC_CONFIG = "offset.storage.topic";
    private static final String OFFSET_STORAGE_TOPIC_CONFIG_DOC = "pulsar topic to store kafka connector offsets in";

    /**
     * <code>topic.namespace</code>
     */
    public static final String TOPIC_NAMESPACE_CONFIG = "topic.namespace";
    private static final String TOPIC_NAMESPACE_CONFIG_DOC = "namespace of topic name to store the output topics";

    static {
        CONFIG = new ConfigDef()
            .define(OFFSET_STORAGE_TOPIC_CONFIG,
                Type.STRING,
                Importance.HIGH,
                OFFSET_STORAGE_TOPIC_CONFIG_DOC)
            .define(TOPIC_NAMESPACE_CONFIG,
                Type.STRING,
                "public/default",
                Importance.HIGH,
                TOPIC_NAMESPACE_CONFIG_DOC);
    }

    public PulsarKafkaWorkerConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
