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
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link Reflections}.
 */
public class SourceConfigUtilsTest {

    @Test
    public void testConvertBackFidelity() throws IOException  {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant("test-tenant");
        sourceConfig.setNamespace("test-namespace");
        sourceConfig.setName("test-source");
        sourceConfig.setArchive("builtin://jdbc");
        sourceConfig.setTopicName("test-output");
        sourceConfig.setSerdeClassName("test-serde");
        sourceConfig.setParallelism(1);
        sourceConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sourceConfig.setConfigs(new HashMap<>());
        Function.FunctionDetails functionDetails = SourceConfigUtils.convert(sourceConfig, null);
        SourceConfig convertedConfig = SourceConfigUtils.convertFromDetails(functionDetails);
        assertEquals(
                new Gson().toJson(sourceConfig),
                new Gson().toJson(convertedConfig)
        );
    }
}
