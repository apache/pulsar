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
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.functions.api.Record;

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Source<T> extends AutoCloseable {

    /**
     * Open connector with configuration.
     *
     * @param config initialization config
     * @param sourceContext environment where the source connector is running
     * @throws Exception IO type exceptions when opening a connector
     */
    void open(Map<String, Object> config, SourceContext sourceContext) throws Exception;

    /**
     * Reads the next message from source.
     * If source does not have any new messages, this call should block.
     * @return next message from source.  The return result should never be null
     * @throws Exception
     */
    Record<T> read() throws Exception;
}
