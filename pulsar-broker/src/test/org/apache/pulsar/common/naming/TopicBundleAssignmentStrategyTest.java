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
package org.apache.pulsar.common.naming;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker-naming")
public class TopicBundleAssignmentStrategyTest {
    @Test
    public void testStrategyFactory() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setTopicBundleAssignmentStrategy(
                "org.apache.pulsar.common.naming.TopicBundleAssignmentStrategyTest$TestStrategy");
        PulsarService pulsarService = mock(PulsarService.class);
        doReturn(conf).when(pulsarService).getConfiguration();
        TopicBundleAssignmentStrategy strategy = TopicBundleAssignmentFactory.create(pulsarService);
        NamespaceBundle bundle = strategy.findBundle(null, null);
        Range<Long> keyRange = Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED);
        String range = String.format("0x%08x_0x%08x", keyRange.lowerEndpoint(), keyRange.upperEndpoint());
        Assert.assertEquals(bundle.getBundleRange(), range);
        Assert.assertEquals(bundle.getNamespaceObject(), NamespaceName.get("my/test"));
    }

    public static class TestStrategy implements TopicBundleAssignmentStrategy {
        @Override
        public NamespaceBundle findBundle(TopicName topicName, NamespaceBundles namespaceBundles) {
            Range<Long> range = Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED);
            return new NamespaceBundle(NamespaceName.get("my/test"), range,
                    mock(NamespaceBundleFactory.class));
        }

        @Override
        public void init(PulsarService pulsarService) {
            
        }
    }
}
