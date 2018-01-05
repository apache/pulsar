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

import io.netty.util.Timer;
import java.net.URL;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.instance.JavaInstanceConfig;
import org.apache.pulsar.functions.runtime.FunctionID;
import org.apache.pulsar.functions.runtime.InstanceID;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link ThreadFunctionContainer}.
 */
@Slf4j
public class ThreadFunctionContainerTest {

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
            1024,
            client);
    }

    @AfterMethod
    public void tearDown() {
        this.factory.close();
    }

    FunctionConfig createFunctionConfig() {
        FunctionConfig config = new FunctionConfig();
        config.setName(TEST_NAME);
        config.setClassName("org.apache.pulsar.functions.runtime.functioncache.AddFunction");
        config.setSourceTopic(TEST_NAME + "-source");
        config.setSinkTopic(TEST_NAME + "-sink");
        return config;
    }

    JavaInstanceConfig createJavaInstanceConfig() {
        JavaInstanceConfig config = new JavaInstanceConfig();

        config.setFunctionConfig(createFunctionConfig());
        config.setFunctionId(new FunctionID());
        config.setFunctionVersion("1.0");
        config.setInstanceId(new InstanceID());
        config.setMaxMemory(2048);
        config.setTimeBudgetInMs(2000);

        return config;
    }

    @Test
    public void testConstructor() {
        JavaInstanceConfig config = createJavaInstanceConfig();

        ThreadFunctionContainer container = factory.createContainer(config, jarFile);
        assertEquals(
            "fn-" + config.getFunctionConfig().getName() + "-instance-" + config.getInstanceId(),
            container.getFnThread().getName());
        container.stop();
        assertFalse(container.getFnThread().isAlive());
    }

}
