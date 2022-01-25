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

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * This is the core interface of the function api. The process is called
 * for every message of the input topic of the function. The incoming input bytes
 * are converted to the input type I for simple Java types(String, Integer, Boolean,
 * Map, and List types) and for org.Json type. If this serialization approach does not
 * meet your needs, you can use the byte stream handler defined in RawRequestHandler.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@FunctionalInterface
public interface Function<I, O> {
    /**
     * Process the input.
     *
     * @return the output
     */
    O process(I input, Context context) throws Exception;

    /**
     * Called once to initialize resources when function instance is started.
     *
     * @param context The Function context
     *
     * @throws Exception
     */
    default void initialize(Context context) throws Exception {}

    /**
     * Called once to properly close resources when function instance is stopped.
     *
     * @throws Exception
     */
    default void close() throws Exception {}
}