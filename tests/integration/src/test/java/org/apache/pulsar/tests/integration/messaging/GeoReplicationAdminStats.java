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
package org.apache.pulsar.tests.integration.messaging;

import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import io.restassured.RestAssured;
import java.io.IOException;
import java.util.List;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/** Raw replication stats shared by the geo-replication and rolling-upgrade E2E suites. */
public final class GeoReplicationAdminStats {

    public static final List<String> TOPIC_FIELDS = List.of("replicatorState", "producerState", "replicatorHealth",
            "lastCursorReadStartedAt", "lastSendStartedAt", "lastReplicationSucceededAt");

    private GeoReplicationAdminStats() {
    }

    // Read the wire response: a typed Admin client can discard unknown properties and omit nulls on reserialization.
    static JsonNode get(PulsarAdmin admin, String topic, String endpoint) throws IOException {
        String path = "/admin/v2/" + TopicName.get(topic).getRestPath() + "/" + endpoint;
        byte[] body = RestAssured.given().baseUri(admin.getServiceUrl()).urlEncodingEnabled(false)
                .get(path).then().statusCode(200).extract().asByteArray();
        return ObjectMapperFactory.getMapper().reader().readTree(body);
    }

    public static JsonNode outbound(PulsarAdmin admin, String topic, String target) throws IOException {
        return get(admin, topic, "stats").path("replication").path(target);
    }

    public static JsonNode internal(PulsarAdmin admin, String topic, String target) throws IOException {
        return get(admin, topic, "internalStats").path("replicators").path(target);
    }

    static void assertInstance(JsonNode instance) {
        for (String field : List.of("producerQueueSize", "producerQueueMaxSize", "sendingMessageSchemaCompatible",
                "lastProducerStartFailure", "lastReadFailure", "lastPublishFailure", "lastProducerRetry",
                "inFlightTasks")) {
            assertTrue(instance.has(field), "Missing " + field + " in " + instance);
        }
        JsonNode max = instance.get("producerQueueMaxSize");
        JsonNode size = instance.get("producerQueueSize");
        assertTrue(max.isIntegralNumber() && max.longValue() > 0, instance.toString());
        assertTrue(size.isNull() || size.isIntegralNumber() && size.longValue() >= 0, instance.toString());
        JsonNode schema = instance.get("sendingMessageSchemaCompatible");
        assertTrue(schema.isNull() || schema.isBoolean(), instance.toString());
        assertTrue(instance.path("inFlightTasks").isArray(), instance.toString());
        for (JsonNode task : instance.path("inFlightTasks")) {
            for (String field : List.of("readPos", "readingEntries", "entries", "completedEntries",
                    "skipReadResultDueToCursorRewind", "readException")) {
                assertTrue(task.has(field), "Missing " + field + " in " + task);
            }
            JsonNode pos = task.get("readPos");
            assertTrue(pos.isNull() || pos.isTextual() && pos.textValue().matches("-?[0-9]+:-?[0-9]+"),
                    task.toString());
            assertTrue(task.get("readingEntries").isIntegralNumber() && task.get("readingEntries").asInt() >= 0);
            JsonNode entries = task.get("entries");
            assertTrue(entries.isNull() || entries.isIntegralNumber() && entries.asInt() >= 0, task.toString());
            assertTrue(task.get("completedEntries").isIntegralNumber() && task.get("completedEntries").asInt() >= 0);
            assertTrue(task.get("skipReadResultDueToCursorRewind").isBoolean());
            assertTrue(task.get("readException").isNull() || task.get("readException").isTextual());
        }
    }

    static void assertUnavailable(JsonNode value) {
        assertTrue(value.isMissingNode() || value.isNull(), value.toString());
    }
}
