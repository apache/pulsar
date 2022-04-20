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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import io.prometheus.client.Counter;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandleStats;
import org.apache.pulsar.common.naming.TopicName;

public class PendingAckHandleStatsImpl implements PendingAckHandleStats {
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static Counter commitTxnCounter;
    private static Counter abortTxnCounter;
    private static boolean _exposeTopicLevelMetrics;

    private final String[] labelSucceed;
    private final String[] labelFailed;

    public PendingAckHandleStatsImpl(String topic, String subscription, boolean exposeTopicLevelMetrics) {
        initialize(exposeTopicLevelMetrics);

        String namespace;
        if (StringUtils.isBlank(topic)) {
            namespace = topic = "unknown";
        } else {
            try {
                namespace = TopicName.get(topic).getNamespace();
            } catch (IllegalArgumentException ex) {
                namespace = "unknown";
            }
        }

        labelSucceed = _exposeTopicLevelMetrics ?
                new String[]{namespace, topic, subscription, "succeed"} : new String[]{"namespace", "succeed"};
        labelFailed = _exposeTopicLevelMetrics ?
                new String[]{namespace, topic, subscription, "failed"} : new String[]{"namespace", "failed"};
    }

    @Override
    public void recordCommitTxn(boolean success) {
        commitTxnCounter.labels(success ? labelSucceed : labelFailed).inc();
    }

    @Override
    public void recordAbortTxn(boolean success) {
        abortTxnCounter.labels(success ? labelSucceed : labelFailed).inc();
    }

    @Override
    public void close() {
        if (_exposeTopicLevelMetrics) {
            commitTxnCounter.remove(this.labelSucceed);
            commitTxnCounter.remove(this.labelFailed);
            abortTxnCounter.remove(this.labelFailed);
            abortTxnCounter.remove(this.labelFailed);
        }
    }

    static void initialize(boolean exposeTopicLevelMetrics) {
        if (INITIALIZED.compareAndSet(false, true)) {
            _exposeTopicLevelMetrics = exposeTopicLevelMetrics;

            String[] labelNames = exposeTopicLevelMetrics ?
                    new String[]{"namespace", "topic", "subscription", "status"} : new String[]{"namespace", "status"};

            commitTxnCounter = Counter
                    .build("pulsar_txn_tp_committed_count", "-")
                    .labelNames(labelNames)
                    .register();

            abortTxnCounter = Counter
                    .build("pulsar_txn_tp_aborted_count", "-")
                    .labelNames(labelNames)
                    .register();
        }
    }
}
