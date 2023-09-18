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
package org.apache.pulsar.broker.transaction;


import io.prometheus.client.CollectorRegistry;
import org.apache.pulsar.broker.transaction.buffer.stats.TxnSnapshotSegmentStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.AssertJUnit;

import static org.junit.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

public class TxnSnapshotSegmentStatsTest {
    private TxnSnapshotSegmentStats stats;
    private final CollectorRegistry registry = new CollectorRegistry();
    private final String namespace = "namespace";
    private final String topicName = "topic_name";

    @Before
    public void setup() {
        stats = new TxnSnapshotSegmentStats(namespace, topicName, registry);
    }

    @After
    public void cleanup() {
        stats.close();
    }

    @Test
    public void testSegmentOperations() {
        stats.recordSegmentOpAddSuccess();
        stats.recordSegmentOpDelSuccess();
        stats.recordSegmentOpReadSuccess();
        stats.recordSegmentOpAddFail();
        stats.recordSegmentOpDelFail();
        stats.recordSegmentOpReadFail();

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_segment_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "add", "success" }), 0.001);

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_segment_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "del", "success" }), 0.001);

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_segment_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "read", "success" }), 0.001);

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_segment_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "add", "fail" }), 0.001);

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_segment_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "del", "fail" }), 0.001);

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_segment_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "read", "fail" }), 0.001);
    }

    @Test
    public void testIndexOperations() {
        stats.recordIndexOpAddSuccess();
        stats.recordIndexOpDelSuccess();
        stats.recordIndexOpReadSuccess();
        stats.recordIndexOpAddFail();
        stats.recordIndexOpDelFail();
        stats.recordIndexOpReadFail();

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_index_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "add", "success" }), 0.001);

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_index_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "del", "success" }), 0.001);

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_index_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "read", "success" }), 0.001);

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_index_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "add", "fail" }), 0.001);

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_index_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "del", "fail" }), 0.001);

        assertEquals(1.0, registry.getSampleValue("pulsar_txn_tb_snapshot_index_op_total",
                new String[]{ "namespace", "topic_name", "op", "status" },
                new String[]{ namespace, topicName, "read", "fail" }), 0.001);
    }

    @Test
    public void testSnapshotOperations() {
        stats.setSnapshotSegmentTotal(5.0);

        assertEquals(5.0, registry.getSampleValue("pulsar_txn_tb_snapshot_index_total",
                new String[]{ "namespace", "topic" },
                new String[]{ namespace, topicName }), 0.001);

    }

    @Test
    public void testClose() {
        // Verify metrics are registered
        assertNotNull(registry.getSampleValue(
                "pulsar_txn_tb_snapshot_segment_op_total",
                new String[]{"namespace", "topic_name", "op", "status"},
                new String[]{namespace, topicName, "add", "success"}));
        assertNotNull(registry.getSampleValue(
                "pulsar_txn_tb_snapshot_index_op_total",
                new String[]{"namespace", "topic_name", "op", "status"},
                new String[]{namespace, topicName, "add", "success"}));
        assertNotNull(registry.getSampleValue(
                "pulsar_txn_tb_snapshot_index_total",
                new String[]{"namespace", "topic"},
                new String[]{namespace, topicName}));
        Double observedSum = registry.getSampleValue(
                "pulsar_txn_tb_snapshot_index_entry_bytes_sum",
                new String[]{"namespace", "topic"},
                new String[]{namespace, topicName});

        assertNotNull(observedSum);

        // Call close
        stats.close();

        // Verify metrics are unregistered
        AssertJUnit.assertNull(registry.getSampleValue(
                "pulsar_txn_tb_snapshot_segment_op_total",
                new String[]{"namespace", "topic_name", "op", "status"},
                new String[]{namespace, topicName, "add", "success"}));
        AssertJUnit.assertNull(registry.getSampleValue(
                "pulsar_txn_tb_snapshot_index_op_total",
                new String[]{"namespace", "topic_name", "op", "status"},
                new String[]{namespace, topicName, "add", "success"}));
        AssertJUnit.assertNull(registry.getSampleValue(
                "pulsar_txn_tb_snapshot_index_total",
                new String[]{"namespace", "topic"},
                new String[]{namespace, topicName}));
        AssertJUnit.assertNull(registry.getSampleValue(
                "pulsar_txn_tb_snapshot_index_entry_bytes_sum",
                new String[]{"namespace", "topic"},
                new String[]{namespace, topicName}));
    }

}
