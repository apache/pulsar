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
import lombok.Getter;
import org.apache.pulsar.common.policies.data.NonPersistentPublisherStats;
import java.util.Objects;

/**
 * Non-persistent publisher statistics.
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class NonPersistentPublisherStatsImpl extends PublisherStatsImpl implements NonPersistentPublisherStats {
    /**
     * for non-persistent topic: broker drops msg if publisher publishes messages more than configured max inflight
     * messages per connection.
     **/
    @Getter
    public double msgDropRate;

    public NonPersistentPublisherStatsImpl add(NonPersistentPublisherStatsImpl stats) {
        Objects.requireNonNull(stats);
        super.add(stats);
        this.msgDropRate += stats.msgDropRate;
        return this;
    }
}