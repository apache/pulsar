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
package org.apache.pulsar.utils;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class Quorum {

    private final int quorum;

    private final Consumer<Boolean> callback;

    private AtomicInteger succeeded = new AtomicInteger(0);

    private AtomicInteger failed = new AtomicInteger(0);

    public Quorum(int quorum, Consumer<Boolean> callback) {
        this.quorum = quorum;
        this.callback = callback;
    }

    private void complete(boolean result) {
        if (callback != null) {
            callback.accept(result);
        }
    }

    public Quorum succeed() {
        if (succeeded.incrementAndGet() >= quorum) {
            complete(true);
        }
        return this;
    }

    public Quorum fail() {
        if (failed.incrementAndGet() >= quorum) {
            complete(false);
        }
        return this;
    }
}
