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
package org.apache.pulsar.io.batchdiscovery;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.BatchSourceTriggerer;
import org.apache.pulsar.io.core.SourceContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

/**
 * This is an implementation of BatchSourceTriggerer that triggers based on a cron expression.
 * BatchSource developers using this should pass the json string of a map that contains
 * "__CRON__" key with the appropriate cron expression. The triggerer will trigger based on this expression.
 *
 */
@Slf4j
public class CronTriggerer implements BatchSourceTriggerer {
  public static final String CRON_KEY = "__CRON__";
  private String cronExpression;
  private ThreadPoolTaskScheduler scheduler;

  @Override
  public void init(Map<String, Object> config, SourceContext sourceContext) {
    if (config == null || config.containsKey(CRON_KEY)) {
      cronExpression = (String) Objects.requireNonNull(config).get(CRON_KEY);
    } else {
      throw new IllegalArgumentException("Cron Trigger is not provided with Cron String");
    }
    scheduler = new ThreadPoolTaskScheduler();
    scheduler.setThreadNamePrefix(String.format("%s/%s/%s-cron-triggerer-",
            sourceContext.getTenant(), sourceContext.getNamespace(), sourceContext.getSourceName()));

    log.info("Initialized CronTrigger with expression: {}", cronExpression);
  }

  @Override
  public void start(Consumer<String> trigger) {

    scheduler.initialize();
    scheduler.schedule(() -> trigger.accept("CRON"), new CronTrigger(cronExpression));
  }

  @Override
  public void stop() {
    if (scheduler != null) {
      scheduler.shutdown();
    }
  }
}

