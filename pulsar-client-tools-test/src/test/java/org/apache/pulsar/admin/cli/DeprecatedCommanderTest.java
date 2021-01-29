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


import com.beust.jcommander.DefaultUsageFormatter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Topics;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class DeprecatedCommanderTest {
    PulsarAdmin admin;
    Topics mockTopics;
    Schemas mockSchemas;
    CmdTopics cmdTopics;

    @BeforeMethod
    public void setup() {
        admin = Mockito.mock(PulsarAdmin.class);
        mockTopics = mock(Topics.class);
        when(admin.topics()).thenReturn(mockTopics);
        mockSchemas = mock(Schemas.class);
        when(admin.schemas()).thenReturn(mockSchemas);
        cmdTopics = new CmdTopics(() -> admin);
    }

    @Test
    public void testDeprecatedCommanderWorks() throws Exception {

        DefaultUsageFormatter defaultUsageFormatter = new DefaultUsageFormatter(cmdTopics.jcommander);
        StringBuilder builder = new StringBuilder();
        defaultUsageFormatter.usage(builder);
        String defaultOutput = builder.toString();

        StringBuilder builder2 = new StringBuilder();
        cmdTopics.jcommander.getUsageFormatter().usage(builder2);
        String outputWithFiltered = builder2.toString();

        assertNotEquals(outputWithFiltered, defaultOutput);
        assertFalse(outputWithFiltered.contains("enable-deduplication"));
        assertTrue(defaultOutput.contains("enable-deduplication"));
        assertFalse(outputWithFiltered.contains("get-max-unacked-messages-on-consumer"));
        assertTrue(defaultOutput.contains("get-max-unacked-messages-on-consumer"));
        assertTrue(outputWithFiltered.contains("get-deduplication"));
        assertTrue(defaultOutput.contains("get-deduplication"));

        // annotation was changed to hidden, reset it.
        cmdTopics = new CmdTopics(() -> admin);
        CmdUsageFormatter formatter = (CmdUsageFormatter)cmdTopics.jcommander.getUsageFormatter();
        formatter.clearDeprecatedCommand();
        StringBuilder builder3 = new StringBuilder();
        cmdTopics.jcommander.getUsageFormatter().usage(builder3);
        String outputAfterClean = builder3.toString();

        assertEquals(outputAfterClean, defaultOutput);

    }

}
