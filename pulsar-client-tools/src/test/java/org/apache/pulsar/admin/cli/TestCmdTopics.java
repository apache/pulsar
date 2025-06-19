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

import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.ListTopicsOptions;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.LedgerInfo;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCmdTopics {

    private static final String PERSISTENT_TOPIC_URL = "persistent://";
    private static final String PARTITIONED_TOPIC_NAME = "my-topic";
    private static final String URL_SLASH = "/";
    private PulsarAdmin pulsarAdmin;
    private CmdTopics cmdTopics;
    private Lookup mockLookup;
    private Topics mockTopics;
    private CmdTopics.PartitionedLookup partitionedLookup;
    private CmdTopics.DeleteCmd deleteCmd;

    @BeforeMethod
    public void setup() throws Exception {
        pulsarAdmin = Mockito.mock(PulsarAdmin.class);
        mockTopics = mock(Topics.class);
        when(pulsarAdmin.topics()).thenReturn(mockTopics);
        Schemas mockSchemas = mock(Schemas.class);
        when(pulsarAdmin.schemas()).thenReturn(mockSchemas);
        mockLookup = mock(Lookup.class);
        when(pulsarAdmin.lookups()).thenReturn(mockLookup);
        when(pulsarAdmin.topics()).thenReturn(mockTopics);
        cmdTopics = spy(new CmdTopics(() -> pulsarAdmin));
        partitionedLookup = spy(cmdTopics.getPartitionedLookup());
        deleteCmd = spy(cmdTopics.getDeleteCmd());
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws IOException {
        //NOTHING FOR NOW
    }

    private static LedgerInfo newLedger(long id, long entries, long size) {
        LedgerInfo l = new LedgerInfo();
        l.ledgerId = id;
        l.entries = entries;
        l.size = size;
        return l;
    }

    @Test
    public void testListCmd() throws Exception {
        List<String> topicList = Lists.newArrayList("persistent://public/default/t1", "persistent://public/default/t2",
                "persistent://public/default/t3");

        Topics topics = mock(Topics.class);
        doReturn(topicList).when(topics).getList(anyString(), any());
        doReturn(topicList).when(topics).getList(anyString(), any(), any(ListTopicsOptions.class));

        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.topics()).thenReturn(topics);

        assertEquals(admin.topics().getList("test", TopicDomain.persistent), topicList);

        CmdTopics cmd = new CmdTopics(() -> admin);
        @Cleanup
        StringWriter stringWriter = new StringWriter();
        @Cleanup
        PrintWriter printWriter = new PrintWriter(stringWriter);
        cmd.getCommander().setOut(printWriter);

        cmd.run("list public/default".split("\\s+"));
        Assert.assertEquals(stringWriter.toString(), String.join("\n", topicList) + "\n");
    }

    @Test
    public void testPartitionedLookup() throws Exception {
        partitionedLookup.topicName = "persistent://public/default/my-topic";
        partitionedLookup.run();
        StringBuilder topic = new StringBuilder();
        topic.append(PERSISTENT_TOPIC_URL);
        topic.append(PUBLIC_TENANT);
        topic.append(URL_SLASH);
        topic.append(DEFAULT_NAMESPACE);
        topic.append(URL_SLASH);
        topic.append(PARTITIONED_TOPIC_NAME);
        verify(mockLookup).lookupPartitionedTopic(eq(topic.toString()));
    }

    @Test
    public void testPartitionedLookupSortByBroker() throws Exception {
        partitionedLookup.topicName = "persistent://public/default/my-topic";
        partitionedLookup.run();
        StringBuilder topic = new StringBuilder();
        topic.append(PERSISTENT_TOPIC_URL);
        topic.append(PUBLIC_TENANT);
        topic.append(URL_SLASH);
        topic.append(DEFAULT_NAMESPACE);
        topic.append(URL_SLASH);
        topic.append(PARTITIONED_TOPIC_NAME);
        partitionedLookup.sortByBroker = true;
        verify(mockLookup).lookupPartitionedTopic(eq(topic.toString()));
    }
    @Test
    public void testRunDeleteSingleTopic() throws PulsarAdminException, IOException {
        // Setup: Specify a single topic to delete
        deleteCmd.topic = "persistent://tenant/namespace/topic";

        // Act: Run the delete command
        deleteCmd.run();

        // Assert: Verify that the delete method was called once for the specified topic
        verify(mockTopics, times(1)).delete("persistent://tenant/namespace/topic", false);
    }

    @Test
    public void testRunDeleteMultipleTopics() throws PulsarAdminException, IOException {
        // Setup: Specify a regex to delete multiple topics
        deleteCmd.topic = "persistent://tenant/namespace/.*";
        deleteCmd.regex = true;

        // Mock: Simulate the return of multiple topics that match the regex
        when(mockTopics.getList("tenant/namespace")).thenReturn(List.of(
                "persistent://tenant/namespace/topic1",
                "persistent://tenant/namespace/topic2"));

        // Act: Run the delete command
        deleteCmd.run();

        // Assert: Verify that the delete method was called once for each of the matching topics
        verify(mockTopics, times(1)).getList("tenant/namespace");
        verify(mockTopics, times(1)).delete("persistent://tenant/namespace/topic1", false);
        verify(mockTopics, times(1)).delete("persistent://tenant/namespace/topic2", false);
    }

    @Test
    public void testRunDeleteTopicsFromFile() throws PulsarAdminException, IOException {
        // Setup: Create a temporary file and write some topics to it
        Path tempFile = Files.createTempFile("topics", ".txt");
        List<String> topics = List.of(
                "persistent://tenant/namespace/topic1",
                "persistent://tenant/namespace/topic2");
        Files.write(tempFile, topics);

        // Setup: Specify the temporary file as input for the delete command
        deleteCmd.topic = tempFile.toString();
        deleteCmd.readFromFile = true;

        // Act: Run the delete command
        deleteCmd.run();

        // Assert: Verify that the delete method was called once for each topic in the file
        for (String topic : topics) {
            verify(mockTopics, times(1)).delete(topic, false);
        }

        // Cleanup: Delete the temporary file
        Files.delete(tempFile);
    }

    @Test
    public void testRunDeleteTopicsFromFileWithException() throws PulsarAdminException, IOException {
        // Setup: Create a temporary file and write some topics to it.
        // Configure the delete method of mockTopics to throw a PulsarAdminException on any input.
        doThrow(new PulsarAdminException("mock fail")).when(mockTopics).delete(anyString(), anyBoolean());
        Path tempFile = Files.createTempFile("topics", ".txt");
        List<String> topics = List.of(
                "persistent://tenant/namespace/topic1",
                "persistent://tenant/namespace/topic2");
        Files.write(tempFile, topics);

        // Setup: Specify the temporary file as input for the delete command
        deleteCmd.topic = tempFile.toString();
        deleteCmd.readFromFile = true;

        // Act: Run the delete command
        // Since we have configured the delete method of mockTopics to throw an exception when called,
        // an exception should be thrown here.
        deleteCmd.run();

        // Assert: Verify that the delete method was called once for each topic in the file,
        // even if one of them threw an exception.
        // This proves that the program continues to attempt to delete the other topics
        // even if an exception occurred while deleting a topic.
        for (String topic : topics) {
            verify(mockTopics, times(1)).delete(topic, false);
        }

        // Cleanup: Delete the temporary file and recreate the mockTopics.
        Files.delete(tempFile);
        mockTopics = mock(Topics.class);
    }

    @Test
    public void testSetRetentionCmd() throws Exception {
        cmdTopics.run("set-retention public/default/topic -s 2T -t 200d".split("\\s+"));
        verify(mockTopics, times(1)).setRetention("persistent://public/default/topic",
                new RetentionPolicies(200 * 24 * 60, 2 * 1024 * 1024));
    }
}
