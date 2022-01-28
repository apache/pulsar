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

import lombok.Data;
import org.apache.pulsar.common.policies.data.CompactionStats;
/**
 * Statistics about compaction.
 */
@Data
public class CompactionStatsImpl implements CompactionStats {

    /** The removed event count of last compaction. */
    public long lastCompactionRemovedEventCount;

    /** The timestamp of last succeed compaction. */
    public long lastCompactionSucceedTimestamp;

    /** The timestamp of last failed compaction. */
    public long lastCompactionFailedTimestamp;

    /** The duration time of last compaction. */
    public long lastCompactionDurationTimeInMills;

    public void reset() {
        this.lastCompactionRemovedEventCount = 0;
        this.lastCompactionSucceedTimestamp = 0;
        this.lastCompactionFailedTimestamp = 0;
        this.lastCompactionDurationTimeInMills = 0;
    }
}
