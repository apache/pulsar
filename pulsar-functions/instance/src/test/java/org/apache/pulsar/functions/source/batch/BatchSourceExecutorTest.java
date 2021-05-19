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
package org.apache.pulsar.functions.source.batch;


import com.google.gson.Gson;
import lombok.Getter;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.io.BatchSourceConfig;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.BatchPushSource;
import org.apache.pulsar.io.core.BatchSource;
import org.apache.pulsar.io.core.BatchSourceTriggerer;
import org.apache.pulsar.io.core.SourceContext;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import static org.testng.Assert.fail;

/**
 * Unit tests for {@link org.apache.pulsar.functions.source.batch.BatchSourceExecutor}
 */
public class BatchSourceExecutorTest {

  public static class TestBatchSource implements BatchSource<String> {
    @Getter
    public static int prepareCount;
    @Getter
    public static int discoverCount;
    @Getter
    public static int recordCount;
    @Getter
    public static int closeCount;
    private Record record = Mockito.mock(Record.class);
    public TestBatchSource() { }

    @Override
    public void open(Map<String, Object> config, SourceContext context) throws Exception {
      if (!config.containsKey("foo")) {
        throw new IllegalArgumentException("Bad config passed to TestBatchSource");
      }
    }

    @Override
    public void discover(Consumer<byte[]> taskEater) throws Exception {
      byte[] retval = new byte[10];
      discoverCount++;
      taskEater.accept(retval);
    }

    @Override
    public void prepare(byte[] task) throws Exception {
      prepareCount++;
    }

    @Override
    public Record<String> readNext() throws Exception {
      if (++recordCount % 5 == 0) {
        return null;
      } else {
        return record;
      }
    }

    @Override
    public void close() throws Exception {
      closeCount++;
    }
  }

  public static class TestBatchSourceFailDiscovery extends TestBatchSource {
    @Override
    public void discover(Consumer<byte[]> taskEater) throws Exception {
      throw new Exception("discovery failed");
    }
  }

  public static class TestBatchPushSource extends BatchPushSource<String> {
    @Getter
    public static int prepareCount;
    @Getter
    public static int discoverCount;
    @Getter
    public static int recordCount;
    @Getter
    public static int closeCount;
    private Record record = Mockito.mock(Record.class);
    public TestBatchPushSource() { }

    @Override
    public void open(Map<String, Object> config, SourceContext context) throws Exception {
      if (!config.containsKey("foo")) {
        throw new IllegalArgumentException("Bad config passed to TestBatchPushSource");
      }
    }

    @Override
    public void discover(Consumer<byte[]> taskEater) throws Exception {
      byte[] retval = new byte[10];
      discoverCount++;
      taskEater.accept(retval);
    }

    @Override
    public void prepare(byte[] task) throws Exception {
      prepareCount++;
      for (int i = 0; i < 5; ++i) {
        consume(record);
        ++recordCount;
      }
      consume(null);
    }

    @Override
    public void close() throws Exception {
      closeCount++;
    }
  }

  public static LinkedBlockingQueue<String> triggerQueue = new LinkedBlockingQueue<>();
  public static LinkedBlockingQueue<String> completedQueue = new LinkedBlockingQueue<>();
  public static class TestDiscoveryTriggerer implements BatchSourceTriggerer {
    @Getter
    private Consumer<String> trigger;
    private Thread thread;

    public TestDiscoveryTriggerer() { }

    @Override
    public void init(Map<String, Object> config, SourceContext sourceContext) throws Exception {
      if (!config.containsKey("DELAY_MS")) {
        throw new IllegalArgumentException("Bad config passed to TestTriggerer");
      }
    }

    @Override
    public void start(Consumer<String> trigger) {

      this.trigger = trigger;
      thread = new Thread(() -> {
        while(true) {
          try {
            trigger.accept(triggerQueue.take());
          } catch (InterruptedException e) {
            break;
          }
        }
      });
      thread.start();
    }

    @Override
    public void stop() {
      if (thread != null) {
        thread.interrupt();
        try {
          thread.join();
        } catch (Exception e) {
        }
      }
    }
  }

  private TestBatchSource testBatchSource;
  private TestBatchPushSource testBatchPushSource;
  private BatchSourceConfig testBatchConfig;
  private Map<String, Object> config;
  private Map<String, Object> pushConfig;
  private BatchSourceExecutor<String> batchSourceExecutor;
  private SourceContext context;
  private ConsumerBuilder consumerBuilder;
  private org.apache.pulsar.client.api.Consumer<byte[]> consumer;
  private TypedMessageBuilder<byte[]> messageBuilder;
  private Message<byte[]> discoveredTask;

  private static Map<String, Object> createConfig(String className, BatchSourceConfig batchConfig) {
    Map<String, Object> config = new HashMap<>();
    config.put("foo", "bar");
    config.put(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY, new Gson().toJson(batchConfig));
    config.put(BatchSourceConfig.BATCHSOURCE_CLASSNAME_KEY, className);
    return config;
  }

  private static BatchSourceConfig createBatchSourceConfig() {
    BatchSourceConfig testBatchConfig = new BatchSourceConfig();
    testBatchConfig.setDiscoveryTriggererClassName(TestDiscoveryTriggerer.class.getName());
    Map<String, Object> triggererConfig = new HashMap<>();
    triggererConfig.put("DELAY_MS", 500);
    testBatchConfig.setDiscoveryTriggererConfig(triggererConfig);
    return testBatchConfig;
  }

  @BeforeMethod
  public void setUp() throws Exception {
    TestBatchSource.closeCount = 0;
    TestBatchSource.discoverCount = 0;
    TestBatchSource.prepareCount = 0;
    TestBatchSource.recordCount = 0;
    TestBatchPushSource.closeCount = 0;
    TestBatchPushSource.discoverCount = 0;
    TestBatchPushSource.prepareCount = 0;
    TestBatchPushSource.recordCount = 0;
    testBatchSource = new TestBatchSource();
    testBatchPushSource = new TestBatchPushSource();
    batchSourceExecutor = new BatchSourceExecutor<>();
    testBatchConfig = createBatchSourceConfig();
    config = createConfig(TestBatchSource.class.getName(), testBatchConfig);
    pushConfig = createConfig(TestBatchPushSource.class.getName(), testBatchConfig);
    context = Mockito.mock(SourceContext.class);
    Mockito.doReturn("test-function").when(context).getSourceName();
    Mockito.doReturn("test-namespace").when(context).getNamespace();
    Mockito.doReturn("test-tenant").when(context).getTenant();
    Mockito.doReturn(0).when(context).getInstanceId();
    consumerBuilder = Mockito.mock(ConsumerBuilder.class);
    Mockito.doReturn(consumerBuilder).when(consumerBuilder).subscriptionName(Mockito.any());
    Mockito.doReturn(consumerBuilder).when(consumerBuilder).subscriptionType(Mockito.any());
    Mockito.doReturn(consumerBuilder).when(consumerBuilder).properties(Mockito.anyMap());
    Mockito.doReturn(consumerBuilder).when(consumerBuilder).topic(Mockito.any());
    discoveredTask = Mockito.mock(Message.class);
    Mockito.doReturn(MessageId.latest).when(discoveredTask).getMessageId();
    consumer = Mockito.mock(org.apache.pulsar.client.api.Consumer.class);
    Mockito.doReturn(discoveredTask).when(consumer).receive();
    Mockito.doReturn(discoveredTask).when(consumer).receive(Mockito.anyInt(), Mockito.any());
    Mockito.doReturn(CompletableFuture.completedFuture(consumer)).when(consumerBuilder).subscribeAsync();
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(consumer).acknowledgeAsync(Mockito.any(MessageId.class));
    Mockito.doReturn(consumerBuilder).when(context).newConsumerBuilder(Schema.BYTES);
    messageBuilder = Mockito.mock(TypedMessageBuilder.class);
    Mockito.doReturn(messageBuilder).when(messageBuilder).value(Mockito.any());
    Mockito.doReturn(messageBuilder).when(messageBuilder).properties(Mockito.any());
    Mockito.doReturn(messageBuilder).when(context).newOutputMessage(Mockito.anyString(), Mockito.any());

    // Discovery
    Mockito.doAnswer((Answer<MessageId>) invocation -> {
      try {
        completedQueue.put("done");
      } catch (Exception e) {
        throw new RuntimeException();
      }
      return null;
    }).when(messageBuilder).send();
    triggerQueue.clear();
    completedQueue.clear();
  }

  @AfterMethod(alwaysRun = true)
  public void cleanUp() throws Exception {
    batchSourceExecutor.close();
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Batch Configs cannot be found")
  public void testWithoutRightConfig() throws Exception {
    config.clear();
    batchSourceExecutor.open(config, context);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Batch Configs cannot be found")
  public void testPushWithoutRightConfig() throws Exception {
    pushConfig.clear();
    batchSourceExecutor.open(pushConfig, context);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "BatchSourceTriggerer does not implement the correct interface")
  public void testWithoutRightTriggerer() throws Exception {
    testBatchConfig.setDiscoveryTriggererClassName(TestBatchSource.class.getName());
    config.put(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY, new Gson().toJson(testBatchConfig));
    batchSourceExecutor.open(config, context);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "BatchSourceTriggerer does not implement the correct interface")
  public void testPushWithoutRightTriggerer() throws Exception {
    testBatchConfig.setDiscoveryTriggererClassName(TestBatchSource.class.getName());
    pushConfig.put(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY, new Gson().toJson(testBatchConfig));
    batchSourceExecutor.open(pushConfig, context);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Bad config passed to TestTriggerer")
  public void testWithoutRightTriggererConfig() throws Exception {
    Map<String, Object> badConfig = new HashMap<>();
    badConfig.put("something", "else");
    testBatchConfig.setDiscoveryTriggererConfig(badConfig);
    config.put(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY, new Gson().toJson(testBatchConfig));
    batchSourceExecutor.open(config, context);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Bad config passed to TestTriggerer")
  public void testPushWithoutRightTriggererConfig() throws Exception {
    Map<String, Object> badConfig = new HashMap<>();
    badConfig.put("something", "else");
    testBatchConfig.setDiscoveryTriggererConfig(badConfig);
    pushConfig.put(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY, new Gson().toJson(testBatchConfig));
    batchSourceExecutor.open(pushConfig, context);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "BatchSource does not implement the correct interface")
  public void testWithoutRightSource() throws Exception {
    config.put(BatchSourceConfig.BATCHSOURCE_CLASSNAME_KEY, TestDiscoveryTriggerer.class.getName());
    batchSourceExecutor.open(config, context);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "BatchSource does not implement the correct interface")
  public void testPushWithoutRightSource() throws Exception {
    pushConfig.put(BatchSourceConfig.BATCHSOURCE_CLASSNAME_KEY, TestDiscoveryTriggerer.class.getName());
    batchSourceExecutor.open(pushConfig, context);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Bad config passed to TestBatchSource")
  public void testWithoutRightSourceConfig() throws Exception {
    config.remove("foo");
    config.put("something", "else");
    batchSourceExecutor.open(config, context);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Bad config passed to TestBatchPushSource")
  public void testPushWithoutRightSourceConfig() throws Exception {
    pushConfig.remove("foo");
    pushConfig.put("something", "else");
    batchSourceExecutor.open(pushConfig, context);
  }

  @Test
  public void testOpenWithRightSource() throws Exception {
    batchSourceExecutor.open(config, context);
  }

  @Test
  public void testPushOpenWithRightSource() throws Exception {
    batchSourceExecutor.open(pushConfig, context);
  }

  @Test
  public void testLifeCycle() throws Exception {
    batchSourceExecutor.open(config, context);
    Assert.assertEquals(testBatchSource.getDiscoverCount(), 0);
    triggerQueue.put("trigger");
    completedQueue.take();
    Assert.assertEquals(testBatchSource.getDiscoverCount(), 1);
    for (int i = 0; i < 5; ++i) {
      batchSourceExecutor.read();
    }
    Assert.assertEquals(testBatchSource.getRecordCount(), 6);
    Assert.assertEquals(testBatchSource.getDiscoverCount(), 1);
    triggerQueue.put("trigger");
    completedQueue.take();
    Assert.assertTrue(testBatchSource.getDiscoverCount() == 2);
    batchSourceExecutor.close();
    Assert.assertEquals(testBatchSource.getCloseCount(), 1);
  }

  @Test
  public void testPushLifeCycle() throws Exception {
    batchSourceExecutor.open(pushConfig, context);
    Assert.assertEquals(testBatchPushSource.getDiscoverCount(), 0);
    triggerQueue.put("trigger");
    completedQueue.take();
    Assert.assertEquals(testBatchPushSource.getDiscoverCount(), 1);
    for (int i = 0; i < 5; ++i) {
      batchSourceExecutor.read();
    }
    Assert.assertEquals(testBatchPushSource.getRecordCount(), 5);
    Assert.assertEquals(testBatchPushSource.getDiscoverCount(), 1);
    triggerQueue.put("trigger");
    completedQueue.take();
    Assert.assertEquals(testBatchPushSource.getDiscoverCount(), 2);
    batchSourceExecutor.close();
    Assert.assertEquals(testBatchPushSource.getCloseCount(), 1);
  }

  @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = "discovery failed")
  public void testDiscoveryPhaseError() throws Exception {
    config = createConfig(TestBatchSourceFailDiscovery.class.getName(), testBatchConfig);
    batchSourceExecutor.open(config, context);
    triggerQueue.put("trigger");
    for (int i = 0; i < 100; i++) {
      batchSourceExecutor.read();
      Thread.sleep(100);
    }
    fail("should have thrown an exception");
  }

}