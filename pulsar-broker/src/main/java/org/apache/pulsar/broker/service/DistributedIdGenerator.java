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
package org.apache.pulsar.broker.service;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate unique ids across multiple nodes.
 * <p>
 * Each node has an instance of this class and uses the same z-node. At startup each node gets assigned a unique id,
 * using ZK sequential nodes.
 * <p>
 * After that, each node can just use a local counter and combine, the application prefix, its own instance id and with
 * the counter incremental value to obtain a globally unique id.
 */
public class DistributedIdGenerator {

    private final String prefix;
    private final long generatorInstanceId;
    private final AtomicLong counter;

    /**
     *
     * @param cs
     *            {@link CoordinationService}
     * @param path
     *            path of the z-node used to track the generators ids
     * @param prefix
     *            prefix to prepend to the generated id. Having a unique prefix can make the id globally unique
     * @throws Exception
     */
    public DistributedIdGenerator(CoordinationService cs, String path, String prefix) throws Exception {
        this.prefix = prefix;
        this.counter = new AtomicLong(0);
        this.generatorInstanceId = cs.getNextCounterValue(path).get();
        log.info("Broker distributed id generator started with instance id {}-{}", prefix, generatorInstanceId);
    }

    public String getNextId() {
        return String.format("%s-%d-%d", prefix, generatorInstanceId, counter.getAndIncrement());
    }

    private static final Logger log = LoggerFactory.getLogger(DistributedIdGenerator.class);
}
