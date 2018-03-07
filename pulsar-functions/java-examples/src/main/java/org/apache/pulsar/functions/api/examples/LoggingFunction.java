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
package org.apache.pulsar.functions.api.examples;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.PulsarFunction;

/**
 * A function with logging example.
 */
public class LoggingFunction implements PulsarFunction<String, String> {

    private static final AtomicIntegerFieldUpdater<LoggingFunction> COUNTER_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(LoggingFunction.class, "counter");
    private volatile int counter = 0;

    @Override
    public String process(String input, Context context) {

        int counterLocal = COUNTER_UPDATER.incrementAndGet(this);
        if ((counterLocal & Integer.MAX_VALUE) % 100000 == 0) {
            context.getLogger().info("Handled {} messages", counterLocal);
        }

        return input + "!";
    }

}
