/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.common.lookup.data;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import com.yahoo.pulsar.common.lookup.data.LookupData;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;

@Test
public class LookupDataTest {

    @Test
    void withConstructor() {
        LookupData data = new LookupData("pulsar://localhost:8888", "pulsar://localhost:8884", "http://localhost:8080");
        assertEquals(data.getBrokerUrl(), "pulsar://localhost:8888");
        assertEquals(data.getHttpUrl(), "http://localhost:8080");
    }

    @SuppressWarnings("unchecked")
    @Test
    void serializeToJsonTest() throws Exception {
        LookupData data = new LookupData("pulsar://localhost:8888", "pulsar://localhost:8884", "http://localhost:8080");
        ObjectMapper mapper = ObjectMapperFactory.getThreadLocal();
        String json = mapper.writeValueAsString(data);

        Map<String, String> jsonMap = mapper.readValue(json, Map.class);

        assertEquals(jsonMap.get("brokerUrl"), "pulsar://localhost:8888");
        assertEquals(jsonMap.get("brokerUrlTls"), "pulsar://localhost:8884");
        assertEquals(jsonMap.get("brokerUrlSsl"), "");
        assertEquals(jsonMap.get("nativeUrl"), "pulsar://localhost:8888");
        assertEquals(jsonMap.get("httpUrl"), "http://localhost:8080");
    }
}
