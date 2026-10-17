/*
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

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.pulsar.io.core.BatchSourceTriggerer;
import org.apache.pulsar.io.core.SourceContext;

/**
 * This is an implementation of BatchSourceTriggerer that triggers based on a cron expression.
 * BatchSource developers using this should pass the json string of a map that contains
 * "__CRON__" key with the appropriate cron expression. The triggerer will trigger based on this expression.
 *
 * <p>The expression uses the six field syntax {@code second minute hour day-of-month month day-of-week},
 * including the {@code L}, {@code W} and {@code #} qualifiers and the {@code @hourly} / {@code @daily} /
 * {@code @weekly} / {@code @monthly} / {@code @yearly} / {@code @annually} / {@code @midnight} macros.
 * Firing times are resolved in the JVM's default time zone.
 */
@CustomLog
public class CronTriggerer implements BatchSourceTriggerer {
  public static final String CRON_KEY = "__CRON__";

  private static final CronParser CRON_PARSER =
          new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53));

  private String cronExpression;
  private ExecutionTime executionTime;
  private ScheduledExecutorService scheduler;

  @Override
  public void init(Map<String, Object> config, SourceContext sourceContext) {
    if (config == null || config.containsKey(CRON_KEY)) {
      cronExpression = (String) Objects.requireNonNull(config).get(CRON_KEY);
    } else {
      throw new IllegalArgumentException("Cron Trigger is not provided with Cron String");
    }
    // Fail on a malformed expression here rather than at the first scheduling attempt.
    executionTime = parse(cronExpression);

    String threadNamePrefix = String.format("%s/%s/%s-cron-triggerer-",
            sourceContext.getTenant(), sourceContext.getNamespace(), sourceContext.getSourceName());
    scheduler = Executors.newSingleThreadScheduledExecutor(newThreadFactory(threadNamePrefix));

    log.info().attr("cronExpression", cronExpression).log("Initialized CronTrigger");
  }

  @Override
  public void start(Consumer<String> trigger) {
    scheduleNext(trigger, ZonedDateTime.now());
  }

  @Override
  public void stop() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  /**
   * Schedules the next firing after {@code from} and re-arms once the trigger returns. Firings therefore
   * never overlap, and a slow trigger delays rather than stacks up the following ones. This is how the
   * previously used Spring {@code CronTrigger} behaved: it derived the next execution from the completion
   * time of the last one.
   */
  private void scheduleNext(Consumer<String> trigger, ZonedDateTime from) {
    Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(from);
    if (nextExecution.isEmpty()) {
      log.warn().attr("cronExpression", cronExpression)
              .log("Cron expression has no further execution time, the triggerer will not fire again");
      return;
    }
    ZonedDateTime scheduledTime = nextExecution.get();
    long delayMillis = Math.max(0, Duration.between(ZonedDateTime.now(), scheduledTime).toMillis());
    try {
      scheduler.schedule(() -> {
        try {
          trigger.accept("CRON");
        } catch (Throwable t) {
          log.error().exception(t).attr("cronExpression", cronExpression).log("Cron trigger failed");
        }
        // Never derive the following execution from an instant before the slot just fired: the executor
        // can wake up a hair early, which would otherwise fire the very same slot a second time.
        ZonedDateTime completionTime = ZonedDateTime.now();
        scheduleNext(trigger, completionTime.isBefore(scheduledTime) ? scheduledTime : completionTime);
      }, delayMillis, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // stop() was called, either before start() got going or while a firing was in flight.
      log.debug().attr("cronExpression", cronExpression).log("Cron triggerer is stopped, not scheduling again");
    }
  }

  /**
   * Parses a cron expression into its schedule. Package private so that the schedule this triggerer
   * derives from an expression can be asserted directly.
   *
   * @throws IllegalArgumentException if the expression is not a valid cron expression
   */
  static ExecutionTime parse(String cronExpression) {
    return ExecutionTime.forCron(CRON_PARSER.parse(normalizeMacro(cronExpression)));
  }

  /**
   * Spring's {@code CronExpression} matched the {@code @daily} style macros case insensitively while
   * cron-utils only accepts them in lower case. Lower casing a leading {@code @} expression keeps such
   * configurations working; any other expression is passed through untouched.
   */
  private static String normalizeMacro(String expression) {
    String trimmed = expression == null ? "" : expression.trim();
    return trimmed.startsWith("@") ? trimmed.toLowerCase(Locale.ROOT) : expression;
  }

  private static ThreadFactory newThreadFactory(String threadNamePrefix) {
    AtomicInteger threadCount = new AtomicInteger(1);
    return runnable -> new Thread(runnable, threadNamePrefix + threadCount.getAndIncrement());
  }
}
