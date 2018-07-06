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
package org.apache.pulsar.io.core;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A simpler version of the Sink interface users can extend for use cases to
 * don't require fine grained delivery control
 */
public abstract class SimpleSink<T> implements Sink<T> {

    @Override
    public void write(RecordContext inputRecordContext, T value) throws Exception {
        write(value)
                .thenAccept(ignored -> inputRecordContext.ack())
                .exceptionally(cause -> {
                    inputRecordContext.fail();
                    return null;
                });
    }

    /**
     * Attempt to publish a type safe collection of messages
     *
     * @param value output value
     * @return Completable future fo async publish request
     */
    public abstract CompletableFuture<Void> write(T value);
}
