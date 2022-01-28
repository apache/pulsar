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
package org.apache.pulsar.io.flume.node;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;
import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestPollingPropertiesFileConfigurationProvider {

    private static final File TESTFILE = new File(
            TestPollingPropertiesFileConfigurationProvider.class.getClassLoader()
                    .getResource("flume-conf.properties").getFile());

    private PollingPropertiesFileConfigurationProvider provider;
    private File baseDir;
    private File configFile;
    private EventBus eventBus;

    @BeforeMethod
    public void setUp() throws Exception {

        baseDir = Files.createTempDir();

        configFile = new File(baseDir, TESTFILE.getName());
        Files.copy(TESTFILE, configFile);

        eventBus = new EventBus("test");
        provider =
                new PollingPropertiesFileConfigurationProvider("host1",
                        configFile, eventBus, 1);
        provider.start();
        LifecycleController.waitForOneOf(provider, LifecycleState.START_OR_ERROR);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(baseDir);
        provider.stop();
    }

    @Test(enabled = false)
    public void testPolling() throws Exception {

        // let first event fire
        Thread.sleep(2000L);

        final List<MaterializedConfiguration> events = Lists.newArrayList();

        Object eventHandler = new Object() {
            @Subscribe
            public synchronized void handleConfigurationEvent(MaterializedConfiguration event) {
                events.add(event);
            }
        };
        eventBus.register(eventHandler);
        configFile.setLastModified(System.currentTimeMillis());

        // now wait for second event to fire
        Thread.sleep(2000L);

        Assert.assertEquals(events.size(), 1, String.valueOf(events));

        MaterializedConfiguration materializedConfiguration = events.remove(0);

        Assert.assertEquals(materializedConfiguration.getSourceRunners().size(),1);
        Assert.assertEquals(materializedConfiguration.getSinkRunners().size(), 1);
        Assert.assertEquals(materializedConfiguration.getChannels().size(), 1);


    }
}
