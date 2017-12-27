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
package org.apache.pulsar.functions.runtime.worker.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public class RestUtils {
    private RestUtils() {

    }

    public static ObjectNode createBaseMessage(String message) {
        final ObjectMapper mapper = new ObjectMapper();
        return mapper.createObjectNode().put("message", message);
    }

    public static String createMessage(String message) {
        return createBaseMessage(message).toString();
    }

    public static String createValidationError(String message, List<String> missing) {
        ObjectNode node = createBaseMessage(message);
        ObjectNode errors = node.putObject("errors");
        ArrayNode missingParameters = errors.putArray("missing_parameters");
        for (String param : missing) {
            missingParameters.add(param);
        }

        return node.toString();
    }

    public static class Message {

        public static class MessageBuilder {
            ObjectNode objectNode = new ObjectMapper().createObjectNode();
            public MessageBuilder add(String key, String value) {
                objectNode.put(key, value);
                return this;
            }

            public String build() {
                return this.objectNode.toString();
            }
        }

        public static MessageBuilder newBuilder() {
            return new MessageBuilder();
        }
    }
}
