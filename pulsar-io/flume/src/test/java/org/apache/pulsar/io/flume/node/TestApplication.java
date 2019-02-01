/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.flume.node;

import static org.mockito.Mockito.*;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;

public class TestApplication {

  private File baseDir;

  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(baseDir);
  }

  private <T extends LifecycleAware> T mockLifeCycle(Class<T> klass) {

    T lifeCycleAware = mock(klass);

    final AtomicReference<LifecycleState> state =
        new AtomicReference<LifecycleState>();

    state.set(LifecycleState.IDLE);

    when(lifeCycleAware.getLifecycleState()).then(new Answer<LifecycleState>() {
      @Override
      public LifecycleState answer(InvocationOnMock invocation)
          throws Throwable {
        return state.get();
      }
    });

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        state.set(LifecycleState.START);
        return null;
      }
    }).when(lifeCycleAware).start();

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        state.set(LifecycleState.STOP);
        return null;
      }
    }).when(lifeCycleAware).stop();

    return lifeCycleAware;
  }

  @Test
  public void testBasicConfiguration() throws Exception {

    EventBus eventBus = new EventBus("test-event-bus");

    MaterializedConfiguration materializedConfiguration = new
        SimpleMaterializedConfiguration();

    SourceRunner sourceRunner = mockLifeCycle(SourceRunner.class);
    materializedConfiguration.addSourceRunner("test", sourceRunner);

    SinkRunner sinkRunner = mockLifeCycle(SinkRunner.class);
    materializedConfiguration.addSinkRunner("test", sinkRunner);

    Channel channel = mockLifeCycle(Channel.class);
    materializedConfiguration.addChannel("test", channel);


    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getConfiguration()).thenReturn(materializedConfiguration);

    Application application = new Application();
    eventBus.register(application);
    eventBus.post(materializedConfiguration);
    application.start();

    Thread.sleep(1000L);

    verify(sourceRunner).start();
    verify(sinkRunner).start();
    verify(channel).start();

    application.stop();

    Thread.sleep(1000L);

    verify(sourceRunner).stop();
    verify(sinkRunner).stop();
    verify(channel).stop();
  }
}
