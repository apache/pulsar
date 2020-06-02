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
package org.apache.pulsar.client.impl;

import com.google.common.collect.Sets;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.testng.Assert.assertEquals;

/**
 * Unit Tests of {@link MultiTopicsConsumerImpl}.
 */
public class MultiTopicsConsumerImplTest {

    @Test
    public void testGetStats() throws Exception {
        String topicName = "test-stats";
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setStatsIntervalSeconds(100);

        ThreadFactory threadFactory = new DefaultThreadFactory("client-test-stats", Thread.currentThread().isDaemon());
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), threadFactory);
        ExecutorService listenerExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);

        PulsarClientImpl clientImpl = new PulsarClientImpl(conf, eventLoopGroup);

        ConsumerConfigurationData consumerConfData = new ConsumerConfigurationData();
        consumerConfData.setTopicNames(Sets.newHashSet(topicName));

        assertEquals(Long.parseLong("100"), clientImpl.getConfiguration().getStatsIntervalSeconds());

        MultiTopicsConsumerImpl impl = new MultiTopicsConsumerImpl(
            clientImpl, consumerConfData,
            listenerExecutor, null, null, null, true);

        impl.getStats();
    }

}
