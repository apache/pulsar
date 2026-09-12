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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.io.core.SourceContext;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for {@link CronTriggerer}.
 *
 * <p>The expected firing times below are the ones the Spring {@code CronTrigger} implementation that this
 * triggerer previously used produced for the same expressions, so they pin down the cron dialect that
 * existing BatchSource configurations rely on.
 */
public class CronTriggererTest {

  private static final ZonedDateTime BASE = ZonedDateTime.parse("2026-08-04T10:15:30Z"); // a Tuesday

  @DataProvider(name = "cronExpressions")
  public Object[][] cronExpressions() {
    return new Object[][]{
        // plain six-field expressions
        {"* * * * * *", "2026-08-04T10:15:31Z"},
        {"0 0/5 * * * *", "2026-08-04T10:20:00Z"},
        {"0 15 10 * * *", "2026-08-05T10:15:00Z"},
        {"0 0 0 * * *", "2026-08-05T00:00:00Z"},
        {"0 0 0 1 * *", "2026-09-01T00:00:00Z"},
        // day-of-week, by name and by number, case insensitive
        {"0 0 0 * * MON", "2026-08-10T00:00:00Z"},
        {"0 0 0 * * mon", "2026-08-10T00:00:00Z"},
        {"0 0 0 * * 1-5", "2026-08-05T00:00:00Z"},
        {"0 0 0 * * SAT,SUN", "2026-08-08T00:00:00Z"},
        // the L / LW / # qualifiers
        {"0 0 0 L * *", "2026-08-31T00:00:00Z"},
        {"0 0 0 LW * *", "2026-08-31T00:00:00Z"},
        {"0 0 0 * * 5#3", "2026-08-21T00:00:00Z"},
        {"0 0 0 * * FRIL", "2026-08-28T00:00:00Z"},
        // leap day, which is only reachable years later
        {"0 0 0 29 2 *", "2028-02-29T00:00:00Z"},
        // macros, which Spring matched case insensitively
        {"@hourly", "2026-08-04T11:00:00Z"},
        {"@HOURLY", "2026-08-04T11:00:00Z"},
        {"@daily", "2026-08-05T00:00:00Z"},
        {"@DAILY", "2026-08-05T00:00:00Z"},
        {"@midnight", "2026-08-05T00:00:00Z"},
        {"@weekly", "2026-08-09T00:00:00Z"},
        {"@monthly", "2026-09-01T00:00:00Z"},
        {"@yearly", "2027-01-01T00:00:00Z"},
        {"@annually", "2027-01-01T00:00:00Z"},
        // surrounding whitespace was tolerated too
        {" 0 0 0 * * * ", "2026-08-05T00:00:00Z"},
    };
  }

  @Test(dataProvider = "cronExpressions")
  public void testNextExecution(String cronExpression, String expectedNextExecution) {
    Optional<ZonedDateTime> next = CronTriggerer.parse(cronExpression).nextExecution(BASE);
    assertThat(next).hasValue(ZonedDateTime.parse(expectedNextExecution));
  }

  @Test
  public void testExpressionThatNeverFiresAgain() {
    // February 30th never comes around.
    assertThat(CronTriggerer.parse("0 0 0 30 2 *").nextExecution(BASE)).isEmpty();
  }

  @Test
  public void testInvalidCronExpressionIsRejectedOnInit() {
    for (String invalid : new String[]{"not-a-cron", "", "0 0 0", "99 0 0 * * *", "@bogus"}) {
      assertThatThrownBy(() -> newTriggerer(invalid))
          .describedAs("expression '%s'", invalid)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void testMissingCronExpressionIsRejectedOnInit() {
    assertThatThrownBy(() -> new CronTriggerer().init(Collections.emptyMap(), newSourceContext()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cron Trigger is not provided with Cron String");
  }

  @Test
  public void testTriggersRepeatedlyOnSchedule() throws Exception {
    CronTriggerer triggerer = newTriggerer("* * * * * *");
    CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();
    try {
      triggerer.start(events::add);
      Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> events.size() >= 3);
    } finally {
      triggerer.stop();
    }
    assertThat(events).containsOnly("CRON");
  }

  @Test
  public void testStopPreventsFurtherTriggers() {
    CronTriggerer triggerer = newTriggerer("* * * * * *");
    AtomicInteger triggerCount = new AtomicInteger();
    triggerer.start(event -> triggerCount.incrementAndGet());
    Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> triggerCount.get() >= 1);

    triggerer.stop();
    int countAtStop = triggerCount.get();
    // Anything already running may still complete, but nothing new may be scheduled after that.
    Awaitility.await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(triggerCount.get()).isLessThanOrEqualTo(countAtStop + 1));
  }

  @Test
  public void testStopIsSafeBeforeStartAndWhenCalledTwice() {
    CronTriggerer triggerer = newTriggerer("* * * * * *");
    assertThatCode(() -> {
      triggerer.stop();
      triggerer.stop();
      // start() after stop() must not blow up, it simply never fires.
      triggerer.start(event -> { });
    }).doesNotThrowAnyException();
  }

  @Test
  public void testTriggerRunsOnANamedThread() throws Exception {
    CronTriggerer triggerer = newTriggerer("* * * * * *");
    CopyOnWriteArrayList<String> threadNames = new CopyOnWriteArrayList<>();
    CountDownLatch fired = new CountDownLatch(1);
    try {
      triggerer.start(event -> {
        threadNames.add(Thread.currentThread().getName());
        fired.countDown();
      });
      assertThat(fired.await(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      triggerer.stop();
    }
    assertThat(threadNames).first().asString().startsWith("test-tenant/test-ns/test-source-cron-triggerer-");
  }

  @Test
  public void testTriggerFailureDoesNotStopTheSchedule() throws Exception {
    CronTriggerer triggerer = newTriggerer("* * * * * *");
    AtomicInteger triggerCount = new AtomicInteger();
    try {
      triggerer.start(event -> {
        triggerCount.incrementAndGet();
        throw new RuntimeException("simulated discovery failure");
      });
      // A throwing trigger must not kill the scheduling loop.
      Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> triggerCount.get() >= 3);
    } finally {
      triggerer.stop();
    }
  }

  @Test
  public void testTriggersDoNotOverlap() throws Exception {
    CronTriggerer triggerer = newTriggerer("* * * * * *");
    AtomicInteger concurrent = new AtomicInteger();
    AtomicInteger maxConcurrent = new AtomicInteger();
    CountDownLatch firedTwice = new CountDownLatch(2);
    try {
      triggerer.start(event -> {
        maxConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
        try {
          // Overrun the one second period so overlapping firings would show up here.
          Thread.sleep(1500);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          concurrent.decrementAndGet();
          firedTwice.countDown();
        }
      });
      assertThat(firedTwice.await(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      triggerer.stop();
    }
    assertThat(maxConcurrent.get()).isEqualTo(1);
  }

  private static CronTriggerer newTriggerer(String cronExpression) {
    CronTriggerer triggerer = new CronTriggerer();
    triggerer.init(Map.of(CronTriggerer.CRON_KEY, cronExpression), newSourceContext());
    return triggerer;
  }

  private static SourceContext newSourceContext() {
    SourceContext sourceContext = mock(SourceContext.class);
    when(sourceContext.getTenant()).thenReturn("test-tenant");
    when(sourceContext.getNamespace()).thenReturn("test-ns");
    when(sourceContext.getSourceName()).thenReturn("test-source");
    return sourceContext;
  }
}
