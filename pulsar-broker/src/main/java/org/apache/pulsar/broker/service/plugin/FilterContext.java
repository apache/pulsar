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
package org.apache.pulsar.broker.service.plugin;

import lombok.Data;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.SubscriptionOption;
import org.apache.pulsar.common.api.proto.MessageMetadata;

@Data
public class FilterContext {
    private EntryBatchSizes batchSizes;
    private SendMessageInfo sendMessageInfo;
    private EntryBatchIndexesAcks indexesAcks;
    private ManagedCursor cursor;
    private boolean isReplayRead;
    private Subscription subscription;
    private SubscriptionOption subscriptionOption;
    private MessageMetadata msgMetadata;

    public void reset() {
        batchSizes = null;
        sendMessageInfo = null;
        indexesAcks = null;
        cursor = null;
        isReplayRead = false;
        subscription = null;
        subscriptionOption = null;
        msgMetadata = null;
    }
}
