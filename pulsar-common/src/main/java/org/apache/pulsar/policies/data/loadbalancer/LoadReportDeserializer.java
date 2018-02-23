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
package org.apache.pulsar.policies.data.loadbalancer;

import java.io.IOException;

import org.apache.pulsar.common.util.ObjectMapperFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LoadReportDeserializer extends JsonDeserializer<LoadManagerReport> {
    @Override
    public LoadManagerReport deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException {
        ObjectMapper mapper = ObjectMapperFactory.getThreadLocal();
        ObjectNode root = ObjectMapperFactory.getThreadLocal().readTree(jsonParser);
        if ((root.has("loadReportType") && root.get("loadReportType").asText().equals(LoadReport.loadReportType))
                || (root.has("underLoaded"))) {
            return mapper.readValue(root.toString(), LoadReport.class);
        } else {
            return mapper.readValue(root.toString(), LocalBrokerData.class);
        }
    }
}