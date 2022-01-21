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

/**
 * An interface for richFunction
 * Extension for Function interface
 * Provide setup and tearDown interface to initial and close any resource used in function
 */
@InterfaceAudience.Public
public interface InitializableFunction<I, O> extends Function<I, O>{

    /**
     * Called when function instance start
     *
     * @param context The Function context
     *
     * @throws Exception
     */
    void initialize(Context context) throws Exception;

    /**
     * Called when function instance close
     *
     * @throws Exception
     */
    void close() throws Exception;
}