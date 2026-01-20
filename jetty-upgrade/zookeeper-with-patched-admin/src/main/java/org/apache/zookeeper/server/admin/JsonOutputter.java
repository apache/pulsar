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
package org.apache.zookeeper.server.admin;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.PrintWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonOutputter implements CommandOutputter {

    static final Logger LOG = LoggerFactory.getLogger(JsonOutputter.class);

    public static final String ERROR_RESPONSE = "{\"error\": \"Exception writing command response to JSON\"}";

    private ObjectMapper mapper;
    private final String clientIP;

    public JsonOutputter(final String clientIP) {
        mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        this.clientIP = clientIP;
    }

    @Override
    public String getContentType() {
        return "application/json";
    }

    @Override
    public void output(CommandResponse response, PrintWriter pw) {
        try {
            mapper.writeValue(pw, response.toMap());
        } catch (JsonGenerationException e) {
            LOG.warn("Exception writing command response to JSON:", e);
            pw.write(ERROR_RESPONSE);
        } catch (JsonMappingException e) {
            LOG.warn("Exception writing command response to JSON:", e);
            pw.write(ERROR_RESPONSE);
        } catch (IOException e) {
            LOG.warn("Exception writing command response as JSON to {}", clientIP, e);
            pw.write(ERROR_RESPONSE);
        }
    }

}
