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

package org.apache.pulsar.functions.runtime.thread;

import io.netty.util.internal.PlatformDependent;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

@PrepareForTest({InstanceUtils.class, PulsarClient.class, PlatformDependent.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*"})
@Slf4j
public class ThreadRuntimeFactoryTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @Test
    public void testMemoryLimitPercent() throws Exception {

        ClientBuilder clientBuilder = testMemoryLimit(null, 50.0);

        Mockito.verify(clientBuilder, Mockito.times(1)).memoryLimit(Mockito.eq((long) (1024 * 0.5)), Mockito.eq(SizeUnit.BYTES));
    }

    @Test
    public void testMemoryLimitAbsolute() throws Exception {

        ClientBuilder clientBuilder = testMemoryLimit(512L, null);

        Mockito.verify(clientBuilder, Mockito.times(1)).memoryLimit(Mockito.eq(512L), Mockito.eq(SizeUnit.BYTES));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMemoryLimitAbsoluteNegative() throws Exception {
        testMemoryLimit(-512L, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMemoryLimitPercentNegative() throws Exception {
        testMemoryLimit(null, -50.0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMemoryLimitPercentOver100() throws Exception {
        testMemoryLimit(null, 120.0);
    }

    @Test
    public void testMemoryLimitNotSet() throws Exception {

        ClientBuilder clientBuilder = testMemoryLimit(null, null);

        Mockito.verify(clientBuilder, Mockito.times(0)).memoryLimit(Mockito.anyLong(), Mockito.any());
    }

    @Test
    public void testMemoryLimitBothSet() throws Exception {

        ClientBuilder clientBuilder = testMemoryLimit(512L, 100.0);

        Mockito.verify(clientBuilder, Mockito.times(1)).memoryLimit(Mockito.eq(512L), Mockito.eq(SizeUnit.BYTES));

        clientBuilder = testMemoryLimit(2048L, 100.0);

        Mockito.verify(clientBuilder, Mockito.times(1)).memoryLimit(Mockito.eq(1024L), Mockito.eq(SizeUnit.BYTES));

        clientBuilder = testMemoryLimit(512L, 25.0);

        Mockito.verify(clientBuilder, Mockito.times(1)).memoryLimit(Mockito.eq(256L), Mockito.eq(SizeUnit.BYTES));

        clientBuilder = testMemoryLimit(512L, 75.0);

        Mockito.verify(clientBuilder, Mockito.times(1)).memoryLimit(Mockito.eq(512L), Mockito.eq(SizeUnit.BYTES));
    }


    private ClientBuilder testMemoryLimit(Long absolute, Double percent) throws Exception {
        PowerMockito.mockStatic(PulsarClient.class);
        PowerMockito.mockStatic(PlatformDependent.class);

        PowerMockito.when(PlatformDependent.maxDirectMemory()).thenReturn(1024L);

        ClientBuilder clientBuilder = Mockito.mock(ClientBuilder.class);
        PowerMockito.when(PulsarClient.builder()).thenReturn(clientBuilder);
        PowerMockito.when(PulsarClient.builder().serviceUrl(Mockito.anyString())).thenReturn(clientBuilder);


        ThreadRuntimeFactoryConfig threadRuntimeFactoryConfig = new ThreadRuntimeFactoryConfig();
        threadRuntimeFactoryConfig.setThreadGroupName("foo");
        ThreadRuntimeFactoryConfig.MemoryLimit memoryLimit = new ThreadRuntimeFactoryConfig.MemoryLimit();
        if (percent != null) {
            memoryLimit.setPercentOfMaxDirectMemory(percent);
        }

        if (absolute != null) {
            memoryLimit.setAbsoluteValue(absolute);
        }
        threadRuntimeFactoryConfig.setPulsarClientMemoryLimit(memoryLimit);

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(ObjectMapperFactory.getThreadLocal().convertValue(threadRuntimeFactoryConfig, Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://broker.pulsar:6650");

        ThreadRuntimeFactory threadRuntimeFactory = new ThreadRuntimeFactory();

        threadRuntimeFactory.initialize(
                workerConfig,
                Mockito.mock(AuthenticationConfig.class),
                Mockito.mock(SecretsProviderConfigurator.class),
                Mockito.mock(ConnectorsManager.class),
                Optional.empty(), Optional.empty());

        return clientBuilder;
    }
}