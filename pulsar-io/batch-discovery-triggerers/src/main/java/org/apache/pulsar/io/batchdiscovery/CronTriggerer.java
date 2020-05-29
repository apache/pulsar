package org.apache.pulsar.io.batchdiscovery;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.*;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

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
    log.info("Initialized CronTrigger with expression: {}", cronExpression);
  }

  @Override
  public void start(Consumer<String> trigger) {
    scheduler = new ThreadPoolTaskScheduler();
    scheduler.initialize();
    scheduler.schedule(() -> trigger.accept("CRON"), new CronTrigger(cronExpression));
  }

  @Override
  public void stop() {
    scheduler.shutdown();
  }

}

