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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.ScalableTopics;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCmdScalableTopics {

    private ScalableTopics scalableTopics;
    private CmdScalableTopics cmdScalableTopics;

    @BeforeMethod
    public void setup() {
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        scalableTopics = mock(ScalableTopics.class);
        when(pulsarAdmin.scalableTopics()).thenReturn(scalableTopics);
        cmdScalableTopics = new CmdScalableTopics(() -> pulsarAdmin);
    }

    @Test
    public void testCreateAcceptsPropertiesBeforeTopic() throws Exception {
        String topic = "topic://public/default/v5-cli-create-before";

        assertTrue(cmdScalableTopics.run(new String[] {
                "create", "-p", "app=cli", "-p", "mode=blackbox", topic
        }));

        verify(scalableTopics).createScalableTopic(eq(topic), eq(1),
                eq(Map.of("app", "cli", "mode", "blackbox")));
    }

    @Test
    public void testCreateAcceptsPropertiesAfterTopic() throws Exception {
        String topic = "topic://public/default/v5-cli-create-after";

        assertTrue(cmdScalableTopics.run(new String[] {
                "create", topic, "-p", "app=cli", "-p", "mode=blackbox"
        }));

        verify(scalableTopics).createScalableTopic(eq(topic), eq(1),
                eq(Map.of("app", "cli", "mode", "blackbox")));
    }

    @Test
    public void testCreateAcceptsCommaSeparatedPropertiesBeforeTopic() throws Exception {
        String topic = "topic://public/default/v5-cli-create-comma-before";

        assertTrue(cmdScalableTopics.run(new String[] {
                "create", "-p", "app=cli,mode=blackbox", topic
        }));

        verify(scalableTopics).createScalableTopic(eq(topic), eq(1),
                eq(Map.of("app", "cli", "mode", "blackbox")));
    }

    @Test
    public void testCreateAcceptsCommaSeparatedPropertiesAfterTopic() throws Exception {
        String topic = "topic://public/default/v5-cli-create-comma-after";

        assertTrue(cmdScalableTopics.run(new String[] {
                "create", topic, "-p", "app=cli,mode=blackbox"
        }));

        verify(scalableTopics).createScalableTopic(eq(topic), eq(1),
                eq(Map.of("app", "cli", "mode", "blackbox")));
    }

    @Test
    public void testListAcceptsPropertiesBeforeNamespace() throws Exception {
        String namespace = "public/default";
        when(scalableTopics.listScalableTopicsByProperties(eq(namespace),
                eq(Map.of("app", "cli", "mode", "blackbox"))))
                .thenReturn(List.of("topic://public/default/v5-cli-list-before"));

        assertTrue(cmdScalableTopics.run(new String[] {
                "list", "-p", "app=cli", "-p", "mode=blackbox", namespace
        }));

        verify(scalableTopics).listScalableTopicsByProperties(eq(namespace),
                eq(Map.of("app", "cli", "mode", "blackbox")));
    }

    @Test
    public void testListAcceptsCommaSeparatedPropertiesBeforeNamespace() throws Exception {
        String namespace = "public/default";
        when(scalableTopics.listScalableTopicsByProperties(eq(namespace),
                eq(Map.of("app", "cli", "mode", "blackbox"))))
                .thenReturn(List.of("topic://public/default/v5-cli-list-comma-before"));

        assertTrue(cmdScalableTopics.run(new String[] {
                "list", "-p", "app=cli,mode=blackbox", namespace
        }));

        verify(scalableTopics).listScalableTopicsByProperties(eq(namespace),
                eq(Map.of("app", "cli", "mode", "blackbox")));
    }

    @Test
    public void testListAcceptsCommaSeparatedPropertiesAfterNamespace() throws Exception {
        String namespace = "public/default";
        when(scalableTopics.listScalableTopicsByProperties(eq(namespace),
                eq(Map.of("app", "cli", "mode", "blackbox"))))
                .thenReturn(List.of("topic://public/default/v5-cli-list-comma-after"));

        assertTrue(cmdScalableTopics.run(new String[] {
                "list", namespace, "-p", "app=cli,mode=blackbox"
        }));

        verify(scalableTopics).listScalableTopicsByProperties(eq(namespace),
                eq(Map.of("app", "cli", "mode", "blackbox")));
    }

    @Test
    public void testListAcceptsPropertiesAfterNamespace() throws Exception {
        String namespace = "public/default";
        when(scalableTopics.listScalableTopicsByProperties(eq(namespace),
                eq(Map.of("app", "cli", "mode", "blackbox"))))
                .thenReturn(List.of("topic://public/default/v5-cli-list-after"));

        assertTrue(cmdScalableTopics.run(new String[] {
                "list", namespace, "-p", "app=cli", "-p", "mode=blackbox"
        }));

        verify(scalableTopics).listScalableTopicsByProperties(eq(namespace),
                eq(Map.of("app", "cli", "mode", "blackbox")));
    }
}
