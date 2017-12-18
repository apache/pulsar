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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.google.common.collect.Lists;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.instance.JavaInstanceConfig;
import org.apache.pulsar.functions.runtime.FunctionID;
import org.apache.pulsar.functions.runtime.InstanceID;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link ThreadFunctionContainer}.
 */
@Slf4j
public class ThreadFunctionContainerTest {

    @Rule
    public final TestName runtime = new TestName();

    private final ThreadFunctionContainerFactory factory;
    private final List<String> jarFiles;
    private final List<URL> classpaths;

    public ThreadFunctionContainerTest() {
        URL jarUrl = getClass().getClassLoader().getResource("multifunction.jar");
        this.jarFiles = Lists.newArrayList(jarUrl.getPath());
        this.classpaths = Collections.emptyList();
        this.factory = new ThreadFunctionContainerFactory();
    }

    @After
    public void tearDown() {
        this.factory.close();
    }

    private Function<Integer, Integer> loadAddFunction() throws Exception {
        Class<? extends Function<Integer, Integer>> cls =
            (Class<? extends Function<Integer, Integer>>)
                Thread.currentThread()
                    .getContextClassLoader()
                    .loadClass("org.apache.pulsar.functions.runtime.functioncache.AddFunction");
        return cls.newInstance();
    }

    FunctionConfig createFunctionConfig() {
        FunctionConfig config = new FunctionConfig();
        config.setName(runtime.getMethodName());
        config.setJarFiles(jarFiles);
        config.setClassName("org.apache.pulsar.functions.runtime.functioncache.AddFunction");
        config.setSourceTopic(runtime.getMethodName() + "-source");
        config.setSinkTopic(runtime.getMethodName() + "-sink");
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

        ThreadFunctionContainer container = factory.createContainer(config);
        assertEquals(
            "fn-" + config.getFunctionConfig().getName() + "-instance-" + config.getInstanceId(),
            container.getFnThread().getName());
        container.stop();
        assertFalse(container.getFnThread().isAlive());
    }

    /*
    @Test
    public void testRunContainer() throws Exception {
        JavaInstanceConfig config = createJavaInstanceConfig();
        CompletableFuture<Integer> future = new CompletableFuture<>();
        Runnable task = () -> {
            try {
                Function<Integer, Integer> func = loadAddFunction();
                future.complete(func.apply(20));
            } catch (Exception e) {
                log.info("Failed to load add function", e);
            }
        };
        ThreadFunctionContainer container = factory.createContainer(config, task);
        container.start();
        container.join();
        assertEquals(40, future.get().intValue());
        container.stop();
    }
    */

}
