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
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.apache.pulsar.common.functions.FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE;
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
        sourceConfig.setRuntimeFlags("-DKerberos");
        sourceConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);

        Map<String, String> consumerConfigs = new HashMap<>();
        consumerConfigs.put("security.protocal", "SASL_PLAINTEXT");
        Map<String, Object> configs = new HashMap<>();
        configs.put("topic", "kafka");
        configs.put("bootstrapServers", "server-1,server-2");
        configs.put("consumerConfigProperties", consumerConfigs);

        sourceConfig.setConfigs(configs);
        Function.FunctionDetails functionDetails = SourceConfigUtils.convert(sourceConfig, new SourceConfigUtils.ExtractedSourceDetails(null, null));
        SourceConfig convertedConfig = SourceConfigUtils.convertFromDetails(functionDetails);

        // add default resources
        sourceConfig.setResources(Resources.getDefaultResources());
        assertEquals(
                new Gson().toJson(sourceConfig),
                new Gson().toJson(convertedConfig)
        );
    }

    @Test
    public void testMergeEqual() {
        SourceConfig sourceConfig = createSourceConfig();
        SourceConfig newSourceConfig = createSourceConfig();
        SourceConfig mergedConfig = SourceConfigUtils.validateUpdate(sourceConfig, newSourceConfig);
        assertEquals(
                new Gson().toJson(sourceConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Function Names differ")
    public void testMergeDifferentName() {
        SourceConfig sourceConfig = createSourceConfig();
        SourceConfig newSourceConfig = createUpdatedSourceConfig("name", "Different");
        SourceConfigUtils.validateUpdate(sourceConfig, newSourceConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Tenants differ")
    public void testMergeDifferentTenant() {
        SourceConfig sourceConfig = createSourceConfig();
        SourceConfig newSourceConfig = createUpdatedSourceConfig("tenant", "Different");
        SourceConfigUtils.validateUpdate(sourceConfig, newSourceConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Namespaces differ")
    public void testMergeDifferentNamespace() {
        SourceConfig sourceConfig = createSourceConfig();
        SourceConfig newSourceConfig = createUpdatedSourceConfig("namespace", "Different");
        SourceConfigUtils.validateUpdate(sourceConfig, newSourceConfig);
    }

    @Test
    public void testMergeDifferentClassName() {
        SourceConfig sourceConfig = createSourceConfig();
        SourceConfig newSourceConfig = createUpdatedSourceConfig("className", "Different");
        SourceConfig mergedConfig = SourceConfigUtils.validateUpdate(sourceConfig, newSourceConfig);
        assertEquals(
                mergedConfig.getClassName(),
                "Different"
        );
        mergedConfig.setClassName(sourceConfig.getClassName());
        assertEquals(
                new Gson().toJson(sourceConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Processing Guarantess cannot be altered")
    public void testMergeDifferentProcessingGuarantees() {
        SourceConfig sourceConfig = createSourceConfig();
        SourceConfig newSourceConfig = createUpdatedSourceConfig("processingGuarantees", EFFECTIVELY_ONCE);
        SourceConfig mergedConfig = SourceConfigUtils.validateUpdate(sourceConfig, newSourceConfig);
    }

    @Test
    public void testMergeDifferentUserConfig() {
        SourceConfig sourceConfig = createSourceConfig();
        Map<String, String> myConfig = new HashMap<>();
        myConfig.put("MyKey", "MyValue");
        SourceConfig newSourceConfig = createUpdatedSourceConfig("configs", myConfig);
        SourceConfig mergedConfig = SourceConfigUtils.validateUpdate(sourceConfig, newSourceConfig);
        assertEquals(
                mergedConfig.getConfigs(),
                myConfig
        );
        mergedConfig.setConfigs(sourceConfig.getConfigs());
        assertEquals(
                new Gson().toJson(sourceConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentSecrets() {
        SourceConfig sourceConfig = createSourceConfig();
        Map<String, String> mySecrets = new HashMap<>();
        mySecrets.put("MyKey", "MyValue");
        SourceConfig newSourceConfig = createUpdatedSourceConfig("secrets", mySecrets);
        SourceConfig mergedConfig = SourceConfigUtils.validateUpdate(sourceConfig, newSourceConfig);
        assertEquals(
                mergedConfig.getSecrets(),
                mySecrets
        );
        mergedConfig.setSecrets(sourceConfig.getSecrets());
        assertEquals(
                new Gson().toJson(sourceConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentParallelism() {
        SourceConfig sourceConfig = createSourceConfig();
        SourceConfig newSourceConfig = createUpdatedSourceConfig("parallelism", 101);
        SourceConfig mergedConfig = SourceConfigUtils.validateUpdate(sourceConfig, newSourceConfig);
        assertEquals(
                mergedConfig.getParallelism(),
                new Integer(101)
        );
        mergedConfig.setParallelism(sourceConfig.getParallelism());
        assertEquals(
                new Gson().toJson(sourceConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentResources() {
        SourceConfig sourceConfig = createSourceConfig();
        Resources resources = new Resources();
        resources.setCpu(0.3);
        resources.setRam(1232l);
        resources.setDisk(123456l);
        SourceConfig newSourceConfig = createUpdatedSourceConfig("resources", resources);
        SourceConfig mergedConfig = SourceConfigUtils.validateUpdate(sourceConfig, newSourceConfig);
        assertEquals(
                mergedConfig.getResources(),
                resources
        );
        mergedConfig.setResources(sourceConfig.getResources());
        assertEquals(
                new Gson().toJson(sourceConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeRuntimeFlags() {
        SourceConfig sourceConfig = createSourceConfig();
        SourceConfig newFunctionConfig = createUpdatedSourceConfig("runtimeFlags", "-Dfoo=bar2");
        SourceConfig mergedConfig = SourceConfigUtils.validateUpdate(sourceConfig, newFunctionConfig);
        assertEquals(
                mergedConfig.getRuntimeFlags(), "-Dfoo=bar2"
        );
        mergedConfig.setRuntimeFlags(sourceConfig.getRuntimeFlags());
        assertEquals(
                new Gson().toJson(sourceConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    private SourceConfig createSourceConfig() {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant("test-tenant");
        sourceConfig.setNamespace("test-namespace");
        sourceConfig.setName("test-source");
        sourceConfig.setParallelism(1);
        sourceConfig.setClassName(IdentityFunction.class.getName());
        sourceConfig.setTopicName("test-output");
        sourceConfig.setSerdeClassName("test-serde");
        sourceConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sourceConfig.setConfigs(new HashMap<>());
        return sourceConfig;
    }

    private SourceConfig createUpdatedSourceConfig(String fieldName, Object fieldValue) {
        SourceConfig sourceConfig = createSourceConfig();
        Class<?> fClass = SourceConfig.class;
        try {
            Field chap = fClass.getDeclaredField(fieldName);
            chap.setAccessible(true);
            chap.set(sourceConfig, fieldValue);
        } catch (Exception e) {
            throw new RuntimeException("Something wrong with the test", e);
        }
        return sourceConfig;
    }
}
