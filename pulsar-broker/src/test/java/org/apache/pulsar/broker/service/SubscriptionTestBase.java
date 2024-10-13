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
package org.apache.pulsar.broker.service;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.testng.annotations.DataProvider;

public abstract class SubscriptionTestBase {

    protected final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    protected final String subName = "subscriptionName";

    @DataProvider(name = "incompatibleKeySharedPolicies")
    public Object[][] incompatibleKeySharedPolicies() {
        KeySharedMeta ksmSticky = new KeySharedMeta().setKeySharedMode(KeySharedMode.STICKY);
        ksmSticky.addHashRange().setStart(0).setEnd(2);

        KeySharedMeta ksmStickyAllowOutOfOrder = new KeySharedMeta().setKeySharedMode(KeySharedMode.STICKY)
                .setAllowOutOfOrderDelivery(true);
        ksmStickyAllowOutOfOrder.addHashRange().setStart(3).setEnd(5);

        KeySharedMeta ksmAutoSplit = new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT);
        KeySharedMeta ksmAutoSplitAllowOutOfOrder = new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT)
                .setAllowOutOfOrderDelivery(true);

        String errorMessageDifferentMode = "Subscription is of different key_shared mode";
        String errorMessageOutOfOrderNotAllowed = "Subscription does not allow out of order delivery";
        String errorMessageOutOfOrderAllowed = "Subscription allows out of order delivery";

        return new Object[][] {
                { ksmAutoSplit, ksmSticky, errorMessageDifferentMode },
                { ksmAutoSplit, ksmStickyAllowOutOfOrder, errorMessageDifferentMode },
                { ksmAutoSplit, ksmAutoSplitAllowOutOfOrder, errorMessageOutOfOrderNotAllowed },

                { ksmAutoSplitAllowOutOfOrder, ksmSticky, errorMessageDifferentMode },
                { ksmAutoSplitAllowOutOfOrder, ksmStickyAllowOutOfOrder, errorMessageDifferentMode },
                { ksmAutoSplitAllowOutOfOrder, ksmAutoSplit, errorMessageOutOfOrderAllowed },

                { ksmSticky, ksmStickyAllowOutOfOrder, errorMessageOutOfOrderNotAllowed },
                { ksmSticky, ksmAutoSplit, errorMessageDifferentMode },
                { ksmSticky, ksmAutoSplitAllowOutOfOrder, errorMessageDifferentMode },

                { ksmStickyAllowOutOfOrder, ksmSticky, errorMessageOutOfOrderAllowed },
                { ksmStickyAllowOutOfOrder, ksmAutoSplit, errorMessageDifferentMode },
                { ksmStickyAllowOutOfOrder, ksmAutoSplitAllowOutOfOrder, errorMessageDifferentMode }
        };
    }

    protected Consumer createKeySharedMockConsumer(String name, KeySharedMeta ksm) {
        Consumer consumer = BrokerTestUtil.createMockConsumer(name);
        doReturn(CommandSubscribe.SubType.Key_Shared).when(consumer).subType();
        doReturn(ksm).when(consumer).getKeySharedMeta();
        doReturn(mock(PendingAcksMap.class)).when(consumer).getPendingAcks();
        return consumer;
    }

}