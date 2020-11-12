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
package org.apache.pulsar.client.api;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Batch message container for individual messages being published until they are batched and sent to broker.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface BatchMessageContainer {

    /**
     * Clear the message batch container.
     */
    void clear();

    /**
     * Check the message batch container is empty.
     *
     * @return return true if empty, otherwise return false.
     */
    boolean isEmpty();

    /**
     * Get count of messages in the message batch container.
     *
     * @return messages count
     */
    int getNumMessagesInBatch();

    /**
     * Get current message batch size of the message batch container in bytes.
     *
     * @return message batch size in bytes
     */
    long getCurrentBatchSize();

    /**
     * Release the payload and clear the container.
     *
     * @param ex cause
     */
    void discard(Exception ex);

    /**
     * Return the batch container batch message in multiple batches.
     * @return
     */
    boolean isMultiBatches();
}
