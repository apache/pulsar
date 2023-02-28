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

package org.apache.pulsar.broker.testcontext;

import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;

/**
 * An in-memory mock bookkeeper which prevents closing when "close" method is called.
 * This is an internal class used by {@link PulsarTestContext}
 *
 * @see PulsarMockBookKeeper
 */
class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

    public NonClosableMockBookKeeper(OrderedExecutor executor) throws Exception {
        super(executor);
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public void shutdown() {
        // no-op
    }

    public void reallyShutdown() {
        super.shutdown();
    }
}
