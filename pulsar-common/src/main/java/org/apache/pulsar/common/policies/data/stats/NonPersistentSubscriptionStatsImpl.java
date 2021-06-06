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
package org.apache.pulsar.common.policies.data.stats;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Objects;
import lombok.Getter;
import org.apache.pulsar.common.policies.data.NonPersistentSubscriptionStats;

/**
 * Statistics for subscription to non-persistent topics.
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class NonPersistentSubscriptionStatsImpl extends SubscriptionStatsImpl implements NonPersistentSubscriptionStats {

    /**
     * for non-persistent topic: broker drops msg for subscription if none of the consumer available for message
     * delivery.
     **/
    @Getter
    public double msgDropRate;

    public void reset() {
        super.reset();
        msgDropRate = 0;
    }

    // if the stats are added for the 1st time, we will need to make a copy of these stats and add it to the current
    // stats
    public NonPersistentSubscriptionStatsImpl add(NonPersistentSubscriptionStatsImpl stats) {
        Objects.requireNonNull(stats);
        super.add(stats);
        this.msgDropRate += stats.msgDropRate;
        return this;
    }
}
