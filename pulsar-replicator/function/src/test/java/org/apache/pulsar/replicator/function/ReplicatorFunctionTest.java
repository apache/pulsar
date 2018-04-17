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
package org.apache.pulsar.replicator.function;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.shaded.com.google.common.collect.Maps;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPoliciesRequest.Action;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.replicator.api.ReplicatorConfig;
import org.apache.pulsar.replicator.api.ReplicatorManager;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 */
public class ReplicatorFunctionTest {

    private String topicName = "persistent://property/cluster/namespace/test";
    private String regionName = "us-east";

    @Test
    public void testInvalidTopic() throws Exception {
        ReplicatorFunction function = new ReplicatorFunction();
        ContextImpl context = createContext();

        ReplicatorTopicData data = new ReplicatorTopicData(Action.Start, topicName, regionName);
        try {
            function.process(data, context);
            Assert.fail("should have failed for missing topicName and regionName in context");
        } catch (IllegalArgumentException e) {// expected
        }

        context.userConfig.put(ReplicatorFunction.CONF_REPLICATION_TOPIC_NAME, topicName);
        try {
            function.process(data, context);
            Assert.fail("should have failed for missing regionName in context");
        } catch (IllegalArgumentException e) {// expected
        }

        context.userConfig.put(ReplicatorFunction.CONF_REPLICATION_TOPIC_NAME, "invalid-topic-name");
        context.userConfig.put(ReplicatorFunction.CONF_REPLICATION_REGION_NAME, regionName);
        Assert.assertFalse(function.process(data, context));
    }

    @Test
    public void testProcess() throws Exception {
        ReplicatorFunction function = spy(new ReplicatorFunction());
        ContextImpl context = createContext();
        context.userConfig.put(ReplicatorFunction.CONF_REPLICATION_TOPIC_NAME, topicName);
        context.userConfig.put(ReplicatorFunction.CONF_REPLICATION_REGION_NAME, regionName);
        context.userConfig.put(ReplicatorFunction.CONF_REPLICATOR_MANAGER_CLASS_NAME,
                ReplicatorManagerImpl.class.getName());
        ReplicatorTopicData data = new ReplicatorTopicData(Action.Start, topicName, regionName);
        Assert.assertTrue(function.process(data, context));
        verify(function, times(1)).startReplicator(any());

        ReplicatorManagerImpl replicatorManager = (ReplicatorManagerImpl) function.getReplicatorManager();
        Assert.assertEquals(replicatorManager.state, State.Started);

        // replicator will not trigger start again if it's already in started mode
        Assert.assertTrue(function.process(data, context));
        verify(function, times(1)).startReplicator(any());

        // close function process
        data.setAction(Action.Stop);
        Assert.assertTrue(function.process(data, context));
        Assert.assertEquals(replicatorManager.state, State.Stopped);
        verify(function, times(1)).close();

        // start again function process
        data.setAction(Action.Start);
        Assert.assertTrue(function.process(data, context));
        replicatorManager = (ReplicatorManagerImpl) function.getReplicatorManager();
        verify(function, times(2)).startReplicator(any());
        Assert.assertEquals(replicatorManager.state, State.Started);

        // restart function process: it stops and restarts the process
        data.setAction(Action.Restart);
        Assert.assertTrue(function.process(data, context));
        replicatorManager = (ReplicatorManagerImpl) function.getReplicatorManager();
        verify(function, times(3)).startReplicator(any());
        verify(function, times(2)).close();
        Assert.assertEquals(replicatorManager.state, State.Started);

    }

    private ContextImpl createContext() {
        ContextImpl context = new ContextImpl();
        context.userConfig = Maps.newHashMap();
        return context;
    }

    public static class ReplicatorManagerImpl implements ReplicatorManager {

        State state = null;

        @Override
        public ReplicatorType getType() {
            return ReplicatorType.Kinesis;
        }

        @Override
        public void start(ReplicatorConfig config) throws Exception {
            state = State.Started;
        }

        @Override
        public void stop() throws Exception {
            state = State.Stopped;
        }
    }

    static enum State {
        Started, Stopped;
    }

    static class ContextImpl implements Context {

        Map<String, String> userConfig;

        @Override
        public CompletableFuture<Void> ack(byte[] arg0, String arg1) {
            return null;
        }

        @Override
        public String getFunctionId() {
            return null;
        }

        @Override
        public String getFunctionName() {
            return null;
        }

        @Override
        public String getFunctionVersion() {
            return null;
        }

        @Override
        public Collection<String> getInputTopics() {
            return null;
        }

        @Override
        public String getInstanceId() {
            return null;
        }

        @Override
        public Logger getLogger() {
            return null;
        }

        @Override
        public byte[] getMessageId() {
            return null;
        }

        @Override
        public String getNamespace() {
            return null;
        }

        @Override
        public String getOutputSerdeClassName() {
            return null;
        }

        @Override
        public String getOutputTopic() {
            return null;
        }

        @Override
        public String getTenant() {
            return null;
        }

        @Override
        public String getTopicName() {
            return null;
        }

        @Override
        public Optional<String> getUserConfigValue(String config) {
            return Optional.ofNullable(this.userConfig.get(config));
        }

        @Override
        public void incrCounter(String arg0, long arg1) {
        }

        @Override
        public <O> CompletableFuture<Void> publish(String arg0, O arg1) {
            return null;
        }

        @Override
        public <O> CompletableFuture<Void> publish(String arg0, O arg1, String arg2) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void recordMetric(String arg0, double arg1) {
        }

        @Override
        public Map<String, String> getUserConfigMap() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getUserConfigValueOrDefault(String arg0, String arg1) {
            // TODO Auto-generated method stub
            return null;
        }
    }

}
