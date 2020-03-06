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
package org.apache.pulsar.functions.api;

/**
 * This interface is a more advanced version of the `Function`, which allows for more complex processing
 * and control of the outgoing message. This enables use cases like controlling the ack behavior as well
 * as dynamically routing the message to an output topic based on the message while
 * still allowing the Functions API to do error handling.
 *
 * All instances of `Function` are wrapped with a default implementation of this interface
 */
public interface RichFunction<I, O> {
    /**
     * Process the input.
     *
     * @return the output, wrapped in an implementation of record
     */
    OutputRecord<I, O> process(Record<I> srcRecord, Context context) throws Exception;
}
