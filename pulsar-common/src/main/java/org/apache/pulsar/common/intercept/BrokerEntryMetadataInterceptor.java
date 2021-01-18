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
package org.apache.pulsar.common.intercept;

import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * A plugin interface that allows you to intercept the client requests to
 *  the Pulsar brokers and add metadata for each entry from broker side.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface BrokerEntryMetadataInterceptor {
    /**
     * Called by ManagedLedger to intercept adding an entry.
     */
    BrokerEntryMetadata intercept(BrokerEntryMetadata brokerMetadata);

    /**
     * Called by ManagedLedger to intercept adding an entry with numberOfMessages.
     */
    BrokerEntryMetadata interceptWithNumberOfMessages(BrokerEntryMetadata brokerMetadata,
            int numberOfMessages);
}
