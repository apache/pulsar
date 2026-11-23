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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.beans.Introspector;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.NonPersistentReplicatorStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;

/** A third-party implementation of the 4.0.13 API. Compile this same source against both SDKs. */
public record LegacyReplicationSnapshot(double getMsgRateIn, long getMsgInCount, double getMsgThroughputIn,
        long getBytesInCount, double getMsgRateOut, long getMsgOutCount, double getMsgThroughputOut,
        long getBytesOutCount, double getMsgRateExpired, long getMsgExpiredCount, long getReplicationBacklog,
        boolean isConnected, long getReplicationDelayInSeconds, String getInboundConnection,
        String getInboundConnectedSince, String getOutboundConnection, String getOutboundConnectedSince)
        implements ReplicatorStats {

    private LegacyReplicationSnapshot(ReplicatorStats stats) {
        this(stats.getMsgRateIn(), stats.getMsgInCount(), stats.getMsgThroughputIn(), stats.getBytesInCount(),
                stats.getMsgRateOut(), stats.getMsgOutCount(), stats.getMsgThroughputOut(), stats.getBytesOutCount(),
                stats.getMsgRateExpired(), stats.getMsgExpiredCount(), stats.getReplicationBacklog(),
                stats.isConnected(), stats.getReplicationDelayInSeconds(), stats.getInboundConnection(),
                stats.getInboundConnectedSince(), stats.getOutboundConnection(), stats.getOutboundConnectedSince());
    }

    public static void main(String[] args) throws Exception {
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build()) {
            ReplicatorStats row = admin.topics().getStats(args[0]).getReplication().get(args[1]);
            var snapshot = new LegacyReplicationSnapshot(row);
            var report = JsonNodeFactory.instance.objectNode();
            report.put("replicationBacklog", snapshot.getReplicationBacklog());
            for (var property : Introspector.getBeanInfo(ReplicatorStats.class).getPropertyDescriptors()) {
                var getter = property.getReadMethod();
                if (getter.isDefault() && getter.invoke(snapshot) == null
                        && getter.equals(NonPersistentReplicatorStats.class.getMethod(getter.getName()))) {
                    report.putNull(property.getName());
                }
            }
            System.out.println(report);
        }
    }
}
