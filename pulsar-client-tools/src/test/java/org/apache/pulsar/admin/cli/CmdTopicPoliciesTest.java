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
package org.apache.pulsar.admin.cli;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.testng.annotations.Test;

public class CmdTopicPoliciesTest {

    @Test
    public void testSetRetentionCmd() throws Exception {
        TopicPolicies topicPolicies = mock(TopicPolicies.class);

        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.topicPolicies(anyBoolean())).thenReturn(topicPolicies);

        CmdTopicPolicies cmd = new CmdTopicPolicies(() -> admin);

        cmd.run("set-retention public/default/topic -s 2T -t 200d".split("\\s+"));

        verify(topicPolicies, times(1)).setRetention("persistent://public/default/topic",
                new RetentionPolicies(200 * 24 * 60, 2 * 1024 * 1024));
    }

    @Test
    public void testSetPersistenceWithDefaultMarkDeleteRate() throws Exception {
        TopicPolicies topicPolicies = mock(TopicPolicies.class);

        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.topicPolicies(anyBoolean())).thenReturn(topicPolicies);

        CmdTopicPolicies cmd = new CmdTopicPolicies(() -> admin);

        // Test that the default value is now -1 (unset) instead of 0
        cmd.run("set-persistence persistent://public/default/topic -e 2 -w 2 -a 2".split("\\s+"));

        verify(topicPolicies, times(1)).setPersistence("persistent://public/default/topic", 
                new PersistencePolicies(2, 2, 2, -1.0, null));
    }

    @Test
    public void testSetPersistenceWithNegativeMarkDeleteRate() throws Exception {
        TopicPolicies topicPolicies = mock(TopicPolicies.class);

        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.topicPolicies(anyBoolean())).thenReturn(topicPolicies);

        CmdTopicPolicies cmd = new CmdTopicPolicies(() -> admin);

        // Test that negative values are now allowed (previously would throw exception)
        cmd.run("set-persistence persistent://public/default/topic -e 2 -w 2 -a 2 -r -5.0".split("\\s+"));

        verify(topicPolicies, times(1)).setPersistence("persistent://public/default/topic", 
                new PersistencePolicies(2, 2, 2, -5.0, null));
    }

    @Test
    public void testSetPersistenceWithZeroMarkDeleteRate() throws Exception {
        TopicPolicies topicPolicies = mock(TopicPolicies.class);

        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.topicPolicies(anyBoolean())).thenReturn(topicPolicies);

        CmdTopicPolicies cmd = new CmdTopicPolicies(() -> admin);

        // Test that zero is still allowed
        cmd.run("set-persistence persistent://public/default/topic -e 2 -w 2 -a 2 -r 0".split("\\s+"));

        verify(topicPolicies, times(1)).setPersistence("persistent://public/default/topic", 
                new PersistencePolicies(2, 2, 2, 0.0, null));
    }

    @Test
    public void testSetPersistenceWithPositiveMarkDeleteRate() throws Exception {
        TopicPolicies topicPolicies = mock(TopicPolicies.class);

        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.topicPolicies(anyBoolean())).thenReturn(topicPolicies);

        CmdTopicPolicies cmd = new CmdTopicPolicies(() -> admin);

        // Test that positive values still work
        cmd.run("set-persistence persistent://public/default/topic -e 2 -w 2 -a 2 -r 10.5".split("\\s+"));

        verify(topicPolicies, times(1)).setPersistence("persistent://public/default/topic", 
                new PersistencePolicies(2, 2, 2, 10.5, null));
    }

    @Test
    public void testSetPersistenceWithUnsetMarkDeleteRate() throws Exception {
        TopicPolicies topicPolicies = mock(TopicPolicies.class);

        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.topicPolicies(anyBoolean())).thenReturn(topicPolicies);

        CmdTopicPolicies cmd = new CmdTopicPolicies(() -> admin);

        // Test explicitly setting to -1 (unset)
        cmd.run("set-persistence persistent://public/default/topic -e 2 -w 2 -a 2 -r -1".split("\\s+"));

        verify(topicPolicies, times(1)).setPersistence("persistent://public/default/topic", 
                new PersistencePolicies(2, 2, 2, -1.0, null));
    }

    @Test
    public void testSetPersistenceWithGlobalFlag() throws Exception {
        TopicPolicies topicPolicies = mock(TopicPolicies.class);

        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.topicPolicies(true)).thenReturn(topicPolicies);

        CmdTopicPolicies cmd = new CmdTopicPolicies(() -> admin);

        // Test with global flag
        cmd.run("set-persistence persistent://public/default/topic -e 2 -w 2 -a 2 -r -1 -g".split("\\s+"));

        verify(topicPolicies, times(1)).setPersistence("persistent://public/default/topic", 
                new PersistencePolicies(2, 2, 2, -1.0, null));
    }
}