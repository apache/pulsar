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
import org.apache.pulsar.common.policies.data.NonPersistentReplicatorStats;

/**
 * Statistics for a non-persistent replicator.
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class NonPersistentReplicatorStatsImpl extends ReplicatorStatsImpl implements NonPersistentReplicatorStats {

    /**
     * for non-persistent topic: broker drops msg for replicator if replicator connection is not writable.
     **/
    @Getter
    public double msgDropRate;

    public NonPersistentReplicatorStatsImpl add(NonPersistentReplicatorStatsImpl stats) {
        Objects.requireNonNull(stats);
        super.add(stats);
        this.msgDropRate += stats.msgDropRate;
        return this;
    }
}
