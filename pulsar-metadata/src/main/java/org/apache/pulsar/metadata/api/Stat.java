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
package org.apache.pulsar.metadata.api;

import lombok.Data;

/**
 * Represent the information associated with a given value in the store.
 */
@Data
public class Stat {

    /**
     * The path of the value.
     */
    final String path;

    /**
     * The data version.
     */
    final long version;

    /**
     * When the value was first inserted.
     */
    final long creationTimestamp;

    /**
     * When the value was last modified.
     */
    final long modificationTimestamp;

    /**
     * Whether the key-value pair is ephemeral or persistent.
     */
    final boolean ephemeral;

    /**
     * Whether the key-value pair had been created within the current "session".
     */
    final boolean createdBySelf;
}
