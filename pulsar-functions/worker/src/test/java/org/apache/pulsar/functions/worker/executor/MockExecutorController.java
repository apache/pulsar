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
package org.apache.pulsar.functions.worker.executor;

import static com.google.common.base.Preconditions.checkArgument;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.mockito.stubbing.Answer;

/**
 * A mocked scheduled executor that records scheduled tasks and executes them when the clock is
 * advanced past their execution time.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PUBLIC)
public class MockExecutorController {

    @Data
    private class DeferredTask implements ScheduledFuture<Void> {

        private final Runnable runnable;
        private final long scheduledAtMillis;
        @Getter
        private final CompletableFuture<Void> future;

        public DeferredTask(Runnable runnable,
                            long delayTimeMs) {
            this.runnable = runnable;
            this.scheduledAtMillis = delayTimeMs + clock.millis();
            this.future = FutureUtils.createFuture();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(scheduledAtMillis - clock.millis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            future.get();
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            future.get(timeout, unit);
            return null;
        }

        void run() {
            runnable.run();
            FutureUtils.complete(future, null);
        }

    }

    @Getter
    private final MockClock clock = new MockClock();
    private final List<DeferredTask> deferredTasks = Lists.newArrayList();

    public MockExecutorController controlSubmit(ScheduledExecutorService service) {
        doAnswer(answerNow()).when(service).submit(any(Runnable.class));
        return this;
    }

    public MockExecutorController controlExecute(ScheduledExecutorService service) {
        doAnswer(answerNow()).when(service).execute(any(Runnable.class));
        return this;
    }

    public MockExecutorController controlSchedule(ScheduledExecutorService service) {
        doAnswer(answerDelay(this)).when(service).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        return this;
    }

    public MockExecutorController controlScheduleAtFixedRate(ScheduledExecutorService service,
                                                             int maxInvocations) {
        doAnswer(answerAtFixedRate(this, maxInvocations))
            .when(service)
            .scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        return this;
    }

    private static Answer<ScheduledFuture<?>> answerAtFixedRate(MockExecutorController controller, int numTimes) {
        return invocationOnMock -> {
            Runnable task = invocationOnMock.getArgument(0);
            long initialDelay = invocationOnMock.getArgument(1);
            long delay = invocationOnMock.getArgument(2);
            TimeUnit unit = invocationOnMock.getArgument(3);

            DeferredTask deferredTask = null;
            for (int i = 0; i < numTimes; i++) {
                long delayMs = unit.toMillis(initialDelay) + i * unit.toMillis(delay);

                deferredTask = controller.addDelayedTask(
                    controller,
                    delayMs,
                    task);
            }
            return deferredTask;
        };
    }

    private static Answer<ScheduledFuture<?>> answerDelay(MockExecutorController executor) {
        return invocationOnMock -> {

           Runnable task = invocationOnMock.getArgument(0);
           long value = invocationOnMock.getArgument(1);
           TimeUnit unit = invocationOnMock.getArgument(2);
           DeferredTask deferredTask = executor.addDelayedTask(executor, unit.toMillis(value), task);
           if (value <= 0) {
               task.run();
               FutureUtils.complete(deferredTask.future, null);
           }
           return deferredTask;
       };
    }

    private static Answer<Future<?>> answerNow() {
        return invocationOnMock -> {

           Runnable task = invocationOnMock.getArgument(0);
           task.run();
           SettableFuture<Void> future = SettableFuture.create();
           future.set(null);
           return future;
       };
    }

    private DeferredTask addDelayedTask(
            MockExecutorController executor,
            long delayTimeMs,
            Runnable task) {
        checkArgument(delayTimeMs >= 0);
        DeferredTask deferredTask = new DeferredTask(task, delayTimeMs);
        executor.deferredTasks.add(deferredTask);
        return deferredTask;
    }

    public void advance(Duration duration) {
        clock.advance(duration);
        Iterator<DeferredTask> entries = deferredTasks.iterator();
        List<DeferredTask> toExecute = Lists.newArrayList();
        while (entries.hasNext()) {
            DeferredTask next = entries.next();
            if (next.getDelay(TimeUnit.MILLISECONDS) <= 0) {
                entries.remove();
                toExecute.add(next);
            }
        }
        for (DeferredTask task : toExecute) {
            task.run();
        }
    }

}
