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
package org.apache.pulsar.functions.stats;

import com.yahoo.sketches.quantiles.DoublesSketch;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.fs.FunctionStats;

/**
 * Function stats.
 */
@Slf4j
public class FunctionStatsImpl implements FunctionStats, AutoCloseable {

    private static final long serialVersionUID = 1L;

    private static final double[] percentiles = { 0.5, 0.95, 0.99, 0.999, 0.9999 };
    private static final DecimalFormat dec = new DecimalFormat("0.000");
    private static final DecimalFormat throughputFormat = new DecimalFormat("0.00");

    private final String functionName;
    private final long statsIntervalSeconds;

    private final LongAdder numMsgsProcess;
    private final LongAdder numMsgsProcessSucceed;
    private final LongAdder numMsgsProcessFailed;
    private final LongAdder totalMsgsProcess;
    private final LongAdder totalMsgsProcessSucceed;
    private final LongAdder totalMsgsProcessFailed;

    private final DoublesSketch ds;

    private long oldTime;
    private final Timer timer;
    private final TimerTask statsTask;
    private Timeout statsTimeout;

    public FunctionStatsImpl(String functionName,
                             long statsIntervalSeconds,
                             Timer timer) {
        this.functionName = functionName;
        this.statsIntervalSeconds = statsIntervalSeconds;
        this.timer = timer;

        this.totalMsgsProcess = new LongAdder();
        this.totalMsgsProcessFailed = new LongAdder();
        this.totalMsgsProcessSucceed = new LongAdder();
        this.numMsgsProcess = new LongAdder();
        this.numMsgsProcessFailed = new LongAdder();
        this.numMsgsProcessSucceed = new LongAdder();
        this.ds = DoublesSketch.builder().build(256);

        this.statsTask = initializeTimerTask();
        this.oldTime = System.nanoTime();
        this.statsTimeout = timer.newTimeout(
            statsTask,
            statsIntervalSeconds,
            TimeUnit.SECONDS);
    }

    private TimerTask initializeTimerTask() {
        return (timeout) -> {

            if (timeout.isCancelled()) {
                return;
            }

            try {
                long now = System.nanoTime();
                double elapsed = (now - oldTime) / 1e9;
                oldTime = now;

                long currentNumMsgsProcessed = numMsgsProcess.sumThenReset();
                long currentNumMsgsProcessFailed = numMsgsProcessFailed.sumThenReset();
                long currentNumMsgsProcessSucceed = numMsgsProcessSucceed.sumThenReset();

                totalMsgsProcess.add(currentNumMsgsProcessed);
                totalMsgsProcessFailed.add(currentNumMsgsProcessFailed);
                totalMsgsProcessSucceed.add(currentNumMsgsProcessSucceed);

                double[] percentileValues;
                synchronized (ds) {
                    percentileValues = ds.getQuantiles(percentiles);
                    ds.reset();
                }

                if ((currentNumMsgsProcessed | currentNumMsgsProcessFailed | currentNumMsgsProcessSucceed) != 0) {

                    for (int i = 0; i < percentileValues.length; i++) {
                        if (percentileValues[i] == Double.NaN) {
                            percentileValues[i] = 0;
                        }
                    }

                    log.info(
                            "[{}] : --- process throughput: {} msg/s --- "
                                    + "Latency: med: {} ms - 95pct: {} ms - 99pct: {} ms - 99.9pct: {} ms - 99.99pct: {} ms --- "
                                    + "Success rate: {} msg/s --- Failures: {} msgs",
                            functionName,
                            throughputFormat.format(currentNumMsgsProcessed / elapsed),
                            dec.format(percentileValues[0] / 1000.0), dec.format(percentileValues[1] / 1000.0),
                            dec.format(percentileValues[2] / 1000.0), dec.format(percentileValues[3] / 1000.0),
                            dec.format(percentileValues[4] / 1000.0),
                            throughputFormat.format(currentNumMsgsProcessSucceed / elapsed), currentNumMsgsProcessFailed);
                }
            } catch (Exception e) {
                log.error("[{}]: {}", functionName, e.getMessage());
            } finally {
                // schedule the next stat timer task
                statsTimeout = timer.newTimeout(
                    statsTask,
                    statsIntervalSeconds,
                    TimeUnit.SECONDS);
            }

        };
    }

    private void cancelStatsTimeout() {
        if (null != statsTimeout) {
            statsTimeout.cancel();
            statsTimeout = null;
        }
    }

    @Override
    public void close() {
        cancelStatsTimeout();
    }

    //
    // Internal use only
    //

    public void incrementProcessFailure() {
        this.numMsgsProcessFailed.increment();
    }

    public void incrementProcessSuccess(long latencyNs) {
        this.numMsgsProcessSucceed.increment();
        synchronized (ds) {
            ds.update(TimeUnit.NANOSECONDS.toMicros(latencyNs));
        }
    }

    public void incrementProcess() {
        this.numMsgsProcess.increment();
    }


    //
    // User Public API
    //

    @Override
    public long getTotalMsgsProcess() {
        return totalMsgsProcess.longValue();
    }

    @Override
    public long getTotalMsgsProcessFailed() {
        return totalMsgsProcessFailed.longValue();
    }

    @Override
    public long getTotalMsgsProcessSucceed() {
        return totalMsgsProcessSucceed.longValue();
    }
}
