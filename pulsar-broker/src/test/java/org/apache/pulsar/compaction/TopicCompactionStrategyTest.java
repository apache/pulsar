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
package org.apache.pulsar.compaction;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker-compaction")
public class TopicCompactionStrategyTest {
    public static class DummyTopicCompactionStrategy implements TopicCompactionStrategy<byte[]> {

        @Override
        public Schema getSchema() {
            return Schema.BYTES;
        }

        @Override
        public boolean shouldKeepLeft(byte[] prev, byte[] cur) {
            return false;
        }
    }


    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLoadInvalidTopicCompactionStrategy() {
        TopicCompactionStrategy.load("uknown");
    }

    @Test
    public void testNumericOrderCompactionStrategy() {
        TopicCompactionStrategy<Integer> strategy =
                TopicCompactionStrategy.load(NumericOrderCompactionStrategy.class.getCanonicalName());
        Assert.assertFalse(strategy.shouldKeepLeft(1, 2));
        Assert.assertTrue(strategy.shouldKeepLeft(2, 1));
    }
}
