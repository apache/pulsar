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

import java.io.Serializable;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Listener on the consumer state changes.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ConsumerEventListener extends Serializable {

    /**
     * Notified when the consumer group is changed, and the consumer becomes the active consumer.
     *
     * @param consumer
     *            the consumer that originated the event
     * @param partitionId
     *            the id of the partition that became active
     */
    void becameActive(Consumer<?> consumer, int partitionId);

    /**
     * Notified when the consumer group is changed, and the consumer is still inactive or becomes inactive.
     *
     * @param consumer
     *            the consumer that originated the event
     * @param partitionId
     *            the id of the partition that became inactive
     */
    void becameInactive(Consumer<?> consumer, int partitionId);

}
