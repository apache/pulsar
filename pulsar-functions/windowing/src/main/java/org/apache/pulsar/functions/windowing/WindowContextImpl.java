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
import org.apache.pulsar.functions.api.WindowContext;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class WindowContextImpl implements WindowContext {

    private Context context;

    public WindowContextImpl(Context context) {
        this.context = context;
    }

    @Override
    public String getTenant() {
        return this.context.getTenant();
    }

    @Override
    public String getNamespace() {
        return this.context.getNamespace();
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
    public int getNumInstances() {
        return this.context.getNumInstances();
    }

    @Override
    public String getFunctionVersion() {
        return this.context.getFunctionVersion();
    }

    @Override
    public Collection<String> getInputTopics() {
        return this.context.getInputTopics();
    }

    @Override
    public String getOutputTopic() {
        return this.context.getOutputTopic();
    }

    @Override
    public String getOutputSchemaType() {
        return this.context.getOutputSchemaType();
    }

    @Override
    public Logger getLogger() {
        return this.context.getLogger();
    }

    @Override
    public void incrCounter(String key, long amount) {
        this.context.incrCounter(key, amount);
    }

    @Override
    public long getCounter(String key) {
        return this.context.getCounter(key);
    }

    @Override
    public void putState(String key, ByteBuffer value) {
        this.context.putState(key, value);
    }

    @Override
    public ByteBuffer getState(String key) {
        return this.context.getState(key);
    }

    @Override
    public Map<String, Object> getUserConfigMap() {
        return this.context.getUserConfigMap();
    }

    @Override
    public Optional<Object> getUserConfigValue(String key) {
        return this.context.getUserConfigValue(key);
    }

    @Override
    public Object getUserConfigValueOrDefault(String key, Object defaultValue) {
        return this.context.getUserConfigValueOrDefault(key, defaultValue);
    }

    @Override
    public void recordMetric(String metricName, double value) {
        this.context.recordMetric(metricName, value);
    }

    @Override
    public <O> CompletableFuture<Void> publish(String topicName, O object) {
        return this.context.publish(topicName, object);
    }

    @Override
    public CompletableFuture<Void> publish(String topicName, Object object, String schemaOrSerdeClassName) {
        return this.context.publish(topicName, object, schemaOrSerdeClassName);
    }
}
