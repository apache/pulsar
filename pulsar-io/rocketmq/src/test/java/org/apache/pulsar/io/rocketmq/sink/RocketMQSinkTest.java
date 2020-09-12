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

package org.apache.pulsar.io.rocketmq.sink;

import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.rocketmq.RocketMQSink;
import org.apache.pulsar.io.rocketmq.RocketMQSinkConfig;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.testng.Assert.*;

public class RocketMQSinkTest {

    @Test
    public void TestOpenAndWriteSink() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put("namesrvAddr", "localhost:9876");
        configs.put("topic", "test2");
        configs.put("tag", "*");
        configs.put("producerGroup", "test-pulsar-io-producer-group");

        RocketMQSink sink = new RocketMQSink();

        // open should success
        sink.open(configs, null);
    }
}
