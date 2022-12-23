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

import java.util.concurrent.ExecutionException;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-compaction")
public class StrategicCompactorTest extends CompactorTest {
    private TopicCompactionStrategy strategy;

    private StrategicTwoPhaseCompactor compactor;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.setup();
        compactor = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler, 1);
        strategy = new TopicCompactionStrategyTest.DummyTopicCompactionStrategy();
    }

    @Override
    protected long compact(String topic) throws ExecutionException, InterruptedException {
        return (long) compactor.compact(topic, strategy).get();
    }

    @Override
    protected Compactor getCompactor() {
        return compactor;
    }
}