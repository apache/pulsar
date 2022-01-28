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
package org.apache.pulsar.broker;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.MessageDeduplication;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.testng.annotations.Test;

public class BrokerMessageDeduplicationTest {

    @Test
    public void markerMessageNotDeduplicated() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration configuration = new ServiceConfiguration();
        doReturn(configuration).when(pulsarService).getConfiguration();
        MessageDeduplication deduplication = spy(new MessageDeduplication(pulsarService,
                mock(PersistentTopic.class), mock(ManagedLedger.class)));
        doReturn(true).when(deduplication).isEnabled();
        Topic.PublishContext context = mock(Topic.PublishContext.class);
        doReturn(true).when(context).isMarkerMessage();
        MessageDeduplication.MessageDupStatus status = deduplication.isDuplicate(context, null);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
    }

    @Test
    public void markerMessageNotRecordPersistent() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration configuration = new ServiceConfiguration();
        doReturn(configuration).when(pulsarService).getConfiguration();
        MessageDeduplication deduplication = spy(new MessageDeduplication(pulsarService,
                mock(PersistentTopic.class), mock(ManagedLedger.class)));
        doReturn(true).when(deduplication).isEnabled();
        Topic.PublishContext context = mock(Topic.PublishContext.class);
         // marker message don't record message persisted.
        doReturn(true).when(context).isMarkerMessage();
        deduplication.recordMessagePersisted(context, null);

        // if is not a marker message, we will get NPE. because context is mocked with null value fields.
        doReturn(false).when(context).isMarkerMessage();
        try {
            deduplication.recordMessagePersisted(context, null);
            fail();
        } catch (Exception npe) {
            assertTrue(npe instanceof NullPointerException);
        }
    }


}
