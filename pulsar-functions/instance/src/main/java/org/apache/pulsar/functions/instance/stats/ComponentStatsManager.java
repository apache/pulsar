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
package org.apache.pulsar.functions.instance.stats;

import com.google.common.collect.EvictingQueue;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;

@Slf4j
public abstract class ComponentStatsManager implements AutoCloseable {

    protected String[] metricsLabels;

    protected ScheduledFuture<?> scheduledFuture;

    protected final FunctionCollectorRegistry collectorRegistry;

    protected final EvictingQueue EMPTY_QUEUE = EvictingQueue.create(0);

    public static final String USER_METRIC_PREFIX = "user_metric_";

    public static final String[] metricsLabelNames = {"tenant", "namespace", "name", "instance_id", "cluster", "fqfn"};

    protected static final String[] exceptionMetricsLabelNames;

    static {
        exceptionMetricsLabelNames = Arrays.copyOf(metricsLabelNames, metricsLabelNames.length + 1);
        exceptionMetricsLabelNames[metricsLabelNames.length] = "error";
    }

    public static ComponentStatsManager getStatsManager(FunctionCollectorRegistry collectorRegistry,
                                  String[] metricsLabels,
                                  ScheduledExecutorService scheduledExecutorService,
                                  Function.FunctionDetails.ComponentType componentType) {
        switch (componentType) {
            case FUNCTION:
                return new FunctionStatsManager(collectorRegistry, metricsLabels, scheduledExecutorService);
            case SOURCE:
                return new SourceStatsManager(collectorRegistry, metricsLabels, scheduledExecutorService);
            case SINK:
                return new SinkStatsManager(collectorRegistry, metricsLabels, scheduledExecutorService);
            default:
                throw new RuntimeException("Unknown component type: " + componentType);
        }
    }

    public ComponentStatsManager(FunctionCollectorRegistry collectorRegistry,
                                 String[] metricsLabels,
                                 ScheduledExecutorService scheduledExecutorService) {

        this.collectorRegistry = collectorRegistry;
        this.metricsLabels = metricsLabels;

        scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                reset();
            } catch (Exception e) {
                log.error("Failed to reset metrics for 1min window", e);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public abstract void reset();

    public abstract void incrTotalReceived();

    public abstract void incrTotalProcessedSuccessfully();

    public abstract void incrSysExceptions(Throwable sysException);

    public abstract void incrUserExceptions(Throwable userException);

    public abstract void incrSourceExceptions(Throwable userException);

    public abstract void incrSinkExceptions(Throwable userException);

    public abstract void setLastInvocation(long ts);

    public abstract void processTimeStart();

    public abstract void processTimeEnd();

    public abstract double getTotalProcessedSuccessfully();

    public abstract double getTotalRecordsReceived();

    public abstract double getTotalSysExceptions();

    public abstract double getTotalUserExceptions();

    public abstract double getLastInvocation();

    public abstract double getAvgProcessLatency();

    public abstract double getTotalProcessedSuccessfully1min();

    public abstract double getTotalRecordsReceived1min();

    public abstract double getTotalSysExceptions1min();

    public abstract double getTotalUserExceptions1min();

    public abstract double getAvgProcessLatency1min();

    public abstract EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestUserExceptions();

    public abstract EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestSystemExceptions();

    public abstract EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestSourceExceptions();

    public abstract EvictingQueue<InstanceCommunication.FunctionStatus.ExceptionInformation> getLatestSinkExceptions();

    public String getStatsAsString() throws IOException {
        StringWriter outputWriter = new StringWriter();

        PrometheusTextFormat.write004(outputWriter, collectorRegistry.metricFamilySamples());

        return outputWriter.toString();
    }

    protected InstanceCommunication.FunctionStatus.ExceptionInformation getExceptionInfo(Throwable th, long ts) {
        InstanceCommunication.FunctionStatus.ExceptionInformation.Builder exceptionInfoBuilder =
                InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder().setMsSinceEpoch(ts);
        String msg = th.getMessage();
        if (msg != null) {
            exceptionInfoBuilder.setExceptionString(msg);
        }
        return exceptionInfoBuilder.build();
    }


    @Override
    public void close() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            scheduledFuture = null;
        }
    }
}
