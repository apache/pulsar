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

import org.apache.pulsar.common.classification.InterfaceAudience.Public;
import org.apache.pulsar.common.classification.InterfaceStability.Evolving;

@Public
@Evolving
public interface StateStore extends AutoCloseable {

    /**
     * The tenant of this store.
     *
     * @return the state store tenant.
     */
    String tenant();

    /**
     * The namespace of this store.
     *
     * @return the state store namespace.
     */
    String namespace();

    /**
     * The name of this store.
     *
     * @return the state store name.
     */
    String name();

    /**
     * The fully qualified state store name.
     *
     * @return the fully qualified state store name.
     */
    String fqsn();

    /**
     * Initialize the state store.
     *
     * @param ctx
     */
    void init(StateStoreContext ctx);

    @Override
    void close();

}
