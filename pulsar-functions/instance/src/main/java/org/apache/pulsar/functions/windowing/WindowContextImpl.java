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
package org.apache.pulsar.functions.windowing;

import org.apache.pulsar.functions.api.Context;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

public class WindowContextImpl implements WindowContext {

    private Context context;

    public WindowContextImpl(Context context) {
        this.context = context;
    }

    @Override
    public String getFunctionName() {
        return this.context.getFunctionName();
    }

    @Override
    public String getFunctionId() {
        return this.context.getFunctionId();
    }

    @Override
    public int getInstanceId() {
        return this.context.getInstanceId();
    }

    @Override
    public String getFunctionVersion() {
        return this.getFunctionVersion();
    }

    @Override
    public long getMemoryLimit() {
        return this.getMemoryLimit();
    }

    @Override
    public long getTimeBudgetInMs() {
        return this.getTimeBudgetInMs();
    }

    @Override
    public long getRemainingTimeInMs() {
        return this.getRemainingTimeInMs();
    }

    @Override
    public Logger getLogger() {
        return this.getLogger();
    }

    @Override
    public String getUserConfigValue(String key) {
        return this.getUserConfigValue(key);
    }

    @Override
    public void recordMetric(String metricName, double value) {
        this.context.recordMetric(metricName, value);
    }

    @Override
    public CompletableFuture<Void> publish(String topicName, Object object, String serDeClassName) {
        return this.context.publish(topicName, object, serDeClassName);
    }
}
