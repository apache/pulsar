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
package org.apache.pulsar.functions.worker.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.ws.rs.core.Response;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.RestException;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RestUtils {

    public static ObjectNode createBaseMessage(String message) {
        final ObjectMapper mapper = ObjectMapperFactory.getMapperWithIncludeAlways().getObjectMapper();
        return mapper.createObjectNode().put("message", message);
    }

    public static String createMessage(String message) {
        return createBaseMessage(message).toString();
    }

    public static void throwUnavailableException() {
        throw new RestException(Response.Status.SERVICE_UNAVAILABLE,
                "Function worker service is not done initializing. " + "Please try again in a little while.");
    }
}
