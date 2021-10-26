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
package org.apache.pulsar.admin.cli;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.LedgerInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCmdTopics {
    private static LedgerInfo newLedger(long id, long entries, long size) {
        LedgerInfo l = new LedgerInfo();
        l.ledgerId = id;
        l.entries = entries;
        l.size = size;
        return l;
    }

    @Test
    public void testFindFirstLedgerWithinThreshold() throws Exception {
        List<LedgerInfo> ledgers = new ArrayList<>();
        ledgers.add(newLedger(0, 10, 1000));
        ledgers.add(newLedger(1, 10, 2000));
        ledgers.add(newLedger(2, 10, 3000));

        // test huge threshold
        Assert.assertNull(CmdTopics.findFirstLedgerWithinThreshold(ledgers, Long.MAX_VALUE));

        // test small threshold
        Assert.assertEquals(CmdTopics.findFirstLedgerWithinThreshold(ledgers, 0),
                            new MessageIdImpl(2, 0, -1));

        // test middling thresholds
        Assert.assertEquals(CmdTopics.findFirstLedgerWithinThreshold(ledgers, 1000),
                            new MessageIdImpl(2, 0, -1));
        Assert.assertEquals(CmdTopics.findFirstLedgerWithinThreshold(ledgers, 5000),
                            new MessageIdImpl(1, 0, -1));
    }

    @Test
    public void testListCmd() throws Exception {
        List<String> topicList = Lists.newArrayList("persistent://public/default/t1", "persistent://public/default/t2",
                "persistent://public/default/t3");

        Topics topics = mock(Topics.class);
        doReturn(topicList).when(topics).getList(anyString(), any());

        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.topics()).thenReturn(topics);

        CmdTopics cmd = new CmdTopics(() -> admin);

        PrintStream defaultSystemOut = System.out;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(); PrintStream ps = new PrintStream(out)) {
            System.setOut(ps);
            cmd.run("list public/default".split("\\s+"));
            Assert.assertEquals(out.toString(), String.join("\n", topicList) + "\n");
        } finally {
            System.setOut(defaultSystemOut);
        }
    }
}
