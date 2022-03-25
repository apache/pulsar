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
package org.apache.pulsar.common.policies;

import java.util.SortedSet;
import org.apache.pulsar.common.policies.data.BrokerStatus;

/**
 * Basic definition of an auto-failover policy.
 */
public abstract class AutoFailoverPolicy {

    /**
     * Checks to see whether the new namespace ownership should be failed over to the secondary brokers.
     *
     * @param brokerStatus
     * @return
     */
    public abstract boolean shouldFailoverToSecondary(SortedSet<BrokerStatus> brokerStatus);

    public abstract boolean shouldFailoverToSecondary(int totalPrimaryCandidates);

    /**
     * Determine whether a broker is considered available or not.
     *
     * @param brokerStatus
     * @return
     */
    public abstract boolean isBrokerAvailable(BrokerStatus brokerStatus);
}
