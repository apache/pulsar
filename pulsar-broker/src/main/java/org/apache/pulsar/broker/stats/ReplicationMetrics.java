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
package org.apache.pulsar.broker.stats;

import com.google.common.collect.Maps;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Map;
import org.apache.pulsar.common.stats.Metrics;

/**
 */
public class ReplicationMetrics {
    public double msgRateOut;
    public double msgThroughputOut;
    public double msgReplBacklog;
    public double maxMsgReplDelayInSeconds;
    public int connected;

    public void reset() {
        msgRateOut = 0;
        msgThroughputOut = 0;
        msgReplBacklog = 0;
        connected = 0;
        maxMsgReplDelayInSeconds = 0;
    }

    public static ReplicationMetrics get() {
        ReplicationMetrics replicationMetrics = RECYCLER.get();
        replicationMetrics.reset();
        return replicationMetrics;
    }

    private final Handle<ReplicationMetrics> recyclerHandle;

    private ReplicationMetrics(Handle<ReplicationMetrics> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<ReplicationMetrics> RECYCLER = new Recycler<ReplicationMetrics>() {
        @Override
        protected ReplicationMetrics newObject(Recycler.Handle<ReplicationMetrics> handle) {
            return new ReplicationMetrics(handle);
        }
    };

    public void recycle() {
        msgRateOut = -1;
        msgThroughputOut = -1;
        msgReplBacklog = -1;
        connected = -1;
        recyclerHandle.recycle(this);
    }

    public Metrics add(String namespace, String local, String remote) {

        Map<String, String> dimensionMap = Maps.newHashMap();
        dimensionMap.put("namespace", namespace);
        dimensionMap.put("from_cluster", local);
        dimensionMap.put("to_cluster", remote);
        Metrics dMetrics = Metrics.create(dimensionMap);

        dMetrics.put("brk_repl_out_rate", msgRateOut);
        dMetrics.put("brk_repl_out_tp_rate", msgThroughputOut);
        dMetrics.put("brk_replication_backlog", msgReplBacklog);
        dMetrics.put("brk_repl_is_connected", connected);
        dMetrics.put("brk_max_replication_delay_second", maxMsgReplDelayInSeconds);

        return dMetrics;

    }
}
