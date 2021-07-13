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
package org.apache.pulsar.functions.instance;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.impl.RoundRobinPartitionMessageRouterImpl;

/**
 * Router for routing function results.
 */
public class FunctionResultRouter extends RoundRobinPartitionMessageRouterImpl {

    private static final FunctionResultRouter INSTANCE = new FunctionResultRouter();

    public FunctionResultRouter() {
        this(Math.abs(ThreadLocalRandom.current().nextInt()), Clock.systemUTC());
    }

    @VisibleForTesting
    public FunctionResultRouter(int startPtnIdx, Clock clock) {
        super(
            HashingScheme.Murmur3_32Hash,
            startPtnIdx,
            true,
            1,
            clock);
    }

    public static FunctionResultRouter of() {
        return INSTANCE;
    }

    @Override
    public int choosePartition(Message msg, TopicMetadata metadata) {
        // if key is specified, we should use key as routing;
        // if key is not specified and no sequence id is provided, not an effectively-once publish, use the default
        // round-robin routing.
        if (msg.hasKey() || msg.getSequenceId() < 0) {
            // TODO: the message key routing is problematic at this moment.
            //       https://github.com/apache/pulsar/pull/1029 is fixing that.
            return super.choosePartition(msg, metadata);
        }
        // if there is no key and sequence id is provided, it is an effectively-once publish, we need to ensure
        // for a given message it always go to one partition, so we use sequence id to do a deterministic routing.
        return (int) (msg.getSequenceId() % metadata.numPartitions());
    }

}
