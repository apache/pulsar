/*
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
package org.apache.pulsar.common.policies.data.stats;

import lombok.Data;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.DrainingHash;

/**
 * Contains information about a draining hash in a Key_Shared subscription.
 * @see ConsumerStats
 */
@Data
public class DrainingHashImpl implements DrainingHash {
    /**
     * Get the sticky key hash value of the draining hash.
     * @return the sticky hash value
     */
    public int hash;
    /**
     * Get number of unacknowledged messages for the draining hash.
     * @return number of unacknowledged messages
     */
    public int unackMsgs;
    /**
     * Get the number of times the hash has blocked an attempted delivery of a message.
     * @return number of times the hash has blocked an attempted delivery of a message
     */
    public int blockedAttempts;
}
