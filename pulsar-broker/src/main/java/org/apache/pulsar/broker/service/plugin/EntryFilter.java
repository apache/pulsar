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

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.common.api.proto.MessageMetadata;

public interface EntryFilter {
    /**
     * Broker will determine whether to filter out this Entry based on the return value of this method.
     * Please do not deserialize the entire Entry in this method,
     * which will have a great impact on Broker's memory and CPU.
     * @param entry
     * @param context
     * @return
     */
    FilterResult filterEntry(Entry entry, FilterContext context);

    class FilterContext {

        EntryBatchSizes batchSizes;
        SendMessageInfo sendMessageInfo;
        EntryBatchIndexesAcks indexesAcks;
        ManagedCursor cursor;
        boolean isReplayRead;
        PersistentSubscription subscription;
        //SubscriptionOption subscriptionOption;
        MessageMetadata msgMetadata;
    }

    enum FilterResult {
        /**
         * deliver to the Consumer
         */
        ACCEPT,
        /**
         * skip the message
         */
        REJECT,
    }
}
