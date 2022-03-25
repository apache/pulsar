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
package org.apache.pulsar.policies.data.loadbalancer;

import java.util.Map;

/**
 * {@link BrokerUsage} object encapsulates the resources that are only used by broker, for now, it's connections both to
 * outside JVM and to the local.
 */
public class BrokerUsage {
    private long connectionCount;
    private long replicationConnectionCount;

    public long getConnectionCount() {
        return connectionCount;
    }

    public long getReplicationConnectionCount() {
        return replicationConnectionCount;
    }

    public void setConnectionCount(long connectionCount) {
        this.connectionCount = connectionCount;
    }

    public void setReplicationConnectionCount(long replicationConnectionCount) {
        this.replicationConnectionCount = replicationConnectionCount;
    }

    /**
     * Factory method that returns an instance of this class populated from metrics we expect the keys that we are
     * looking there's no explicit type checked object which guarantees that we have a specific type of metrics.
     *
     * @param metrics metrics object containing the metrics collected per minute from mbeans
     *
     * @return new instance of the class populated from metrics
     */
    public static BrokerUsage populateFrom(Map<String, Object> metrics) {
        BrokerUsage brokerUsage = null;
        if (metrics.containsKey("brk_conn_cnt")) {
            brokerUsage = new BrokerUsage();
            brokerUsage.connectionCount = (Long) metrics.get("brk_conn_cnt");
        }
        if (metrics.containsKey("brk_repl_conn_cnt")) {
            if (brokerUsage == null) {
                brokerUsage = new BrokerUsage();
            }
            brokerUsage.replicationConnectionCount = (Long) metrics.get("brk_repl_conn_cnt");
        }
        return brokerUsage;
    }
}
