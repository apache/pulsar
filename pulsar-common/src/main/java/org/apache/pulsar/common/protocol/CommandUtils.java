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
package org.apache.pulsar.common.protocol;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pulsar.common.api.proto.PulsarApi;

/**
 * Helper class to work with commands.
 */
public final class CommandUtils {

    private CommandUtils() {}

    public static Map<String, String> metadataFromCommand(PulsarApi.CommandProducer commandProducer) {
        return toMap(commandProducer.getMetadataList());
    }

    public static Map<String, String> metadataFromCommand(PulsarApi.CommandSubscribe commandSubscribe) {
        return toMap(commandSubscribe.getMetadataList());
    }

    static List<PulsarApi.KeyValue> toKeyValueList(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return Collections.emptyList();
        }

        return metadata.entrySet().stream().map(e ->
                PulsarApi.KeyValue.newBuilder().setKey(e.getKey()).setValue(e.getValue()).build())
                .collect(Collectors.toList());
    }

    private static Map<String, String> toMap(List<PulsarApi.KeyValue> keyValues) {
        if (keyValues == null || keyValues.isEmpty()) {
            return Collections.emptyMap();
        }

        return keyValues.stream()
                .collect(Collectors.toMap(PulsarApi.KeyValue::getKey, PulsarApi.KeyValue::getValue));
    }
}
