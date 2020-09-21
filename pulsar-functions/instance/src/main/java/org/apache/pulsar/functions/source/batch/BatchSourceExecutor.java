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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.io.BatchSourceConfig;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.Actions;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.io.core.BatchSource;
import org.apache.pulsar.io.core.BatchSourceTriggerer;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * BatchSourceExecutor wraps BatchSource as Source. Thus from Pulsar IO perspective, it is running a regular
 * streaming source. The BatchSourceExecutor orchestrates the lifecycle of BatchSource.
 *
 * The current implementation uses an intermediate topic between the discovery process and the actual batchsource
 * instances. The Discovery is run on 0th instance. Any tasks discovered during the discover are written to the
 * intermediate topic. All the instances consume tasks from this intermediate topic using a shared subscription.
 */

@Slf4j
public class BatchSourceExecutor<T> implements Source<T> {

  private Map<String, Object> config;
  private SourceContext sourceContext;
  private BatchSourceTriggerer discoveryTriggerer; // Only init in instance 0
  private Consumer<byte[]> intermediateTopicConsumer;
  private Message<byte[]> currentTask;
  private BatchSourceConfig batchSourceConfig;
  private String batchSourceClassName;
  private BatchSource<T> batchSource;
  private String intermediateTopicName;

  @Override
  public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
    this.config = config;
    this.sourceContext = sourceContext;
    this.intermediateTopicName = SourceConfigUtils.computeBatchSourceIntermediateTopicName(sourceContext.getTenant(),
            sourceContext.getNamespace(), sourceContext.getSourceName()).toString();
    this.getBatchSourceConfigs(config);
    this.initializeBatchSource();
    this.start();
  }

  @Override
  public Record<T> read() throws Exception {
    while (true) {
      if (currentTask == null) {
        retrieveNextTask();
        prepareInternal();
      }
      Record<T> retval = batchSource.readNext();
      if (retval == null) {
        // signals end if this batch
        intermediateTopicConsumer.acknowledge(currentTask.getMessageId());
        currentTask = null;
      } else {
        return retval;
      }
    }
  }

  private void getBatchSourceConfigs(Map<String, Object> config) {
    if (!config.containsKey(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY)
        || !config.containsKey(BatchSourceConfig.BATCHSOURCE_CLASSNAME_KEY)) {
      throw new IllegalArgumentException("Batch Configs cannot be found");
    }

    String batchSourceConfigJson = (String) config.get(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY);
    this.batchSourceConfig = new Gson().fromJson(batchSourceConfigJson, BatchSourceConfig.class);
    this.batchSourceClassName = (String)config.get(BatchSourceConfig.BATCHSOURCE_CLASSNAME_KEY);
  }

  private void initializeBatchSource() {
    // First init the batchsource
    ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
    Object userClassObject = Reflections.createInstance(
            batchSourceClassName,
            clsLoader);
    if (userClassObject instanceof BatchSource) {
        batchSource = (BatchSource) userClassObject;
    } else {
      throw new IllegalArgumentException("BatchSource does not implement the correct interface");
    }

    // next init the discovery triggerer
    Object discoveryClassObject = Reflections.createInstance(
            batchSourceConfig.getDiscoveryTriggererClassName(),
            clsLoader);
    if (discoveryClassObject instanceof BatchSourceTriggerer) {
      discoveryTriggerer = (BatchSourceTriggerer) discoveryClassObject;
    } else {
      throw new IllegalArgumentException("BatchSourceTriggerer does not implement the correct interface");
    }
  }

  private void start() throws Exception {
    createIntermediateTopicConsumer();
    batchSource.open(this.config, this.sourceContext);
    if (sourceContext.getInstanceId() == 0) {
      discoveryTriggerer.init(batchSourceConfig.getDiscoveryTriggererConfig(),
                              this.sourceContext);
      discoveryTriggerer.start(this::triggerDiscover);
    }
  }

  private void triggerDiscover(String discoveredEvent) {
    try {
      batchSource.discover((task) -> this.taskEater(discoveredEvent, task));
    } catch (Exception e) {
      log.error("Error on discover", e);
      throw new RuntimeException(e);
    }
  }

  private void taskEater(String discoveredEvent, byte[] task) {
    try {
      Map<String, String> properties = new HashMap<>();
      properties.put("discoveredEvent", discoveredEvent);
      properties.put("produceTime", String.valueOf(System.currentTimeMillis()));
      TypedMessageBuilder<byte[]> message = sourceContext.newOutputMessage(intermediateTopicName, Schema.BYTES);
      message.value(task).properties(properties);
      message.send();
    } catch (Exception e) {
      log.error("error writing discovered task to intermediate topic", e);
      throw new RuntimeException("error writing discovered task to intermediate topic");
    }
  }

  private void prepareInternal() {
    try {
      batchSource.prepare(currentTask.getValue());
    } catch (Exception e) {
      log.error("Error on prepare", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    this.stop();
  }

  private void stop() throws Exception {
    Exception ex = null;
    if (discoveryTriggerer != null) {
      try {
        discoveryTriggerer.stop();
      } catch (Exception e) {
        log.error("Encountered exception when closing Batch Source Triggerer", e);
        ex = e;
      }
      discoveryTriggerer = null;
    }
    if (intermediateTopicConsumer != null) {
      try {
        intermediateTopicConsumer.close();
      } catch (Exception e) {
        log.error("Encountered exception when closing intermediate topic of Batch Source", e);
        if (ex != null) {
          ex = e;
        }
      }
      intermediateTopicConsumer = null;
    }
    if (batchSource != null) {
      try {
        batchSource.close();
      } catch (Exception e) {
        log.error("Encountered exception when closing Batch Source", e);
        if (ex != null) {
          ex = e;
        }
      }
    }

    if (ex != null) {
      throw ex;
    }
  }

  private void createIntermediateTopicConsumer() {
    String subName = SourceConfigUtils.computeBatchSourceInstanceSubscriptionName(
      sourceContext.getTenant(), sourceContext.getNamespace(),
      sourceContext.getSourceName());
    String fqfn = FunctionCommon.getFullyQualifiedName(
      sourceContext.getTenant(), sourceContext.getNamespace(),
      sourceContext.getSourceName());
    try {
      Actions.newBuilder()
        .addAction(
          Actions.Action.builder()
            .actionName(String.format("Setting up instance consumer for BatchSource intermediate " +
              "topic for function %s", fqfn))
            .numRetries(10)
            .sleepBetweenInvocationsMs(1000)
            .supplier(() -> {
              try {
                CompletableFuture<Consumer<byte[]>> cf = sourceContext.newConsumerBuilder(Schema.BYTES)
                  .subscriptionName(subName)
                  .subscriptionType(SubscriptionType.Shared)
                  .topic(intermediateTopicName)
                  .properties(InstanceUtils.getProperties(
                    Function.FunctionDetails.ComponentType.SOURCE, fqfn, sourceContext.getInstanceId()))
                  .subscribeAsync();
                intermediateTopicConsumer = cf.join();
                return Actions.ActionResult.builder()
                  .success(true)
                  .build();
              } catch (Exception e) {
                return Actions.ActionResult.builder()
                  .success(false)
                  .errorMsg(e.getMessage())
                  .build();
              }
            })
            .build())
        .run();
    } catch (InterruptedException e) {
      log.error("Error setting up instance subscription for intermediate topic", e);
      throw new RuntimeException(e);
    }
  }


  private void retrieveNextTask() throws Exception {
    currentTask = intermediateTopicConsumer.receive();
    return;
  }

}

