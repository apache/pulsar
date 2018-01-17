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

package org.apache.pulsar.functions.runtime.container;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.apache.pulsar.shade.io.netty.util.Timer;
import java.net.URL;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.functions.fs.LimitsConfig;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link ThreadFunctionContainer}.
 */
@Slf4j
public class ThreadFunctionContainerTest {

    private static final String TEST_TENANT = "test-function-tenant";
    private static final String TEST_NAMESPACE = "test-function-namespace";
    private static final String TEST_NAME = "test-function-container";

    private final PulsarClientImpl client;
    private final ThreadFunctionContainerFactory factory;
    private final String jarFile;

    public ThreadFunctionContainerTest() {
        URL jarUrl = getClass().getClassLoader().getResource("multifunction.jar");
        this.jarFile = jarUrl.getPath();
        this.client = mock(PulsarClientImpl.class);
        when(client.getConfiguration()).thenReturn(new ClientConfiguration());
        when(client.timer()).thenReturn(mock(Timer.class));

        this.factory = new ThreadFunctionContainerFactory(
            "ThreadFunctionContainerFactory",
            1024,
            client);
    }

    @AfterMethod
    public void tearDown() {
        this.factory.close();
    }

    FunctionConfig createFunctionConfig() {
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        functionConfigBuilder.setTenant(TEST_TENANT);
        functionConfigBuilder.setNamespace(TEST_NAMESPACE);
        functionConfigBuilder.setName(TEST_NAME);
        functionConfigBuilder.setClassName("org.apache.pulsar.functions.runtime.functioncache.AddFunction");
        functionConfigBuilder.putInputs(TEST_NAME + "-source", "org.apache.pulsar.functions.api.utils.Utf8StringSerDe");
        functionConfigBuilder.setSinkTopic(TEST_NAME + "-sink");
        return functionConfigBuilder.build();
    }

    InstanceConfig createJavaInstanceConfig() {
        InstanceConfig config = new InstanceConfig();

        config.setFunctionConfig(createFunctionConfig());
        config.setFunctionId(java.util.UUID.randomUUID().toString());
        config.setFunctionVersion("1.0");
        config.setInstanceId(java.util.UUID.randomUUID().toString());
        LimitsConfig limitsConfig = new LimitsConfig();
        limitsConfig.setMaxTimeMs(2000);
        limitsConfig.setMaxMemoryMb(2048);
        config.setLimitsConfig(limitsConfig);

        return config;
    }

    @Test
    public void testConstructor() {
        InstanceConfig config = createJavaInstanceConfig();

        ThreadFunctionContainer container = factory.createContainer(config, jarFile);
        assertEquals(TEST_TENANT + "/" + TEST_NAMESPACE + "/" + TEST_NAME,
                container.getFnThread().getName());
        assertFalse(container.getFnThread().isAlive());
    }

}
