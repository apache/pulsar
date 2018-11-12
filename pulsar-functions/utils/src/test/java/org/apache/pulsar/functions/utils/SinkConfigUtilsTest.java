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
package org.apache.pulsar.functions.utils;

import com.google.gson.Gson;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.functions.proto.Function;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link Reflections}.
 */
public class SinkConfigUtilsTest {

    @Test
    public void testConvertBackFidelity() throws IOException  {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant("test-tenant");
        sinkConfig.setNamespace("test-namespace");
        sinkConfig.setName("test-source");
        sinkConfig.setParallelism(1);
        sinkConfig.setArchive("builtin://jdbc");
        sinkConfig.setSourceSubscriptionName("test-subscription");
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(true).serdeClassName("test-serde").build());
        sinkConfig.setInputSpecs(inputSpecs);
        sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sinkConfig.setConfigs(new HashMap<>());
        sinkConfig.setRetainOrdering(false);
        sinkConfig.setAutoAck(true);
        sinkConfig.setTimeoutMs(2000l);
        Function.FunctionDetails functionDetails = SinkConfigUtils.convert(sinkConfig, null);
        SinkConfig convertedConfig = SinkConfigUtils.convertFromDetails(functionDetails);
        assertEquals(
                new Gson().toJson(sinkConfig),
                new Gson().toJson(convertedConfig)
        );
    }
}
