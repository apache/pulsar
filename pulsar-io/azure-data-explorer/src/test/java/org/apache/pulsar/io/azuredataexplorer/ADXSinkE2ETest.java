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
package org.apache.pulsar.io.azuredataexplorer;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ADXSinkE2ETest {

    @BeforeMethod
    public void setUp() throws Exception {
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
    }

    @Test
    public void TestOpenAndWriteSink() throws Exception {


        Map<String, Object> configs = new HashMap<>();
        configs.put("clusterUrl", System.getenv("kustoCluster"));
        configs.put("database", System.getenv("kustoDatabase"));
        configs.put("table", "ADXPulsarData");
        configs.put("batchTimeMs", 1000);
        configs.put("flushImmediately", true);
        configs.put("appId", System.getenv("kustoAadAppId"));
        configs.put("appKey", System.getenv("kustoAadAppSecret"));
        configs.put("tenantId", System.getenv("kustoAadAuthorityID"));

        ADXSink sink = new ADXSink();
        sink.open(configs, null);

        for (int i = 0; i < 50; i++) {
            Record<byte[]> record = build("key_"+i, "test data from ADX Pulsar Sink_"+i);
            sink.write(record);
        }
        Thread.sleep(40000);
        sink.close();
    }

    private Record<byte[]> build(String key, String value) {
        SinkRecord<byte[]> record = new SinkRecord<>(new Record<>() {

            @Override
            public byte[] getValue() {
                return value.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Optional<String> getDestinationTopic() {
                return Optional.of("destination-topic");
            }

            @Override
            public Optional<Long> getEventTime(){
                return  Optional.of(System.currentTimeMillis());
            }

            @Override
            public Optional<String> getKey() {
                return Optional.of("key-" + key);
            }

            @Override
            public Map<String, String> getProperties() {
                return new HashMap<String, String>();
            }
        }, value.getBytes(StandardCharsets.UTF_8));

        return record;
    }
}
