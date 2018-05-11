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
package org.apache.pulsar.functions.sink;

import org.apache.pulsar.io.core.RecordContext;
import org.apache.pulsar.io.core.Sink;

/**
 * This class extends connect sink.
 *
 * <p>Runtime should interact sink rather than interact directly to the public {@link Sink} interface.
 *
 * <p>There is a default implementation provided for wrapping up the user provided {@link Sink}. Pulsar sink
 * should be implemented using this interface to ensure supporting effective-once.
 */
public interface RuntimeSink<T> extends Sink<T>{

    /**
     * Write the <tt>value</tt>value.
     *
     * <p>The implementation of this class is responsible for notifying the runtime whether the input record
     * for generating this value is done with processing by {@link RecordContext#ack} and {@link RecordContext#fail}.
     *
     * @param inputRecordContext input record context
     * @param value output value computed from the runtime.
     */
    default void write(RecordContext inputRecordContext, T value) throws Exception {
        write(value)
            .thenAccept(ignored -> inputRecordContext.ack())
            .exceptionally(cause -> {
                inputRecordContext.fail();
                return null;
            });
    }
}
