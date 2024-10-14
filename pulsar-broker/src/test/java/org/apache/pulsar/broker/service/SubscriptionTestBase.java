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

        String errorMessagePrefix = "Subscription is of different type. ";
        String errorMessageSubscriptionModeSticky = errorMessagePrefix + "Active subscription key_shared mode of "
                + "'STICKY' is different than the connecting consumer's key_shared mode 'AUTO_SPLIT'.";
        String errorMessageSubscriptionModeAutoSplit = errorMessagePrefix + "Active subscription key_shared mode of "
                + "'AUTO_SPLIT' is different than the connecting consumer's key_shared mode 'STICKY'.";
        String errorMessageOutOfOrderNotAllowed = errorMessagePrefix + "Active subscription does not allow out of "
                + "order delivery while the connecting consumer allows it.";
        String errorMessageOutOfOrderAllowed = errorMessagePrefix + "Active subscription allows out of order delivery "
                + "while the connecting consumer does not allow it.";

        return new Object[][] {
                { ksmAutoSplit, ksmSticky, errorMessageSubscriptionModeAutoSplit },
                { ksmAutoSplit, ksmStickyAllowOutOfOrder, errorMessageSubscriptionModeAutoSplit },
                { ksmAutoSplit, ksmAutoSplitAllowOutOfOrder, errorMessageOutOfOrderNotAllowed },

                { ksmAutoSplitAllowOutOfOrder, ksmSticky, errorMessageSubscriptionModeAutoSplit },
                { ksmAutoSplitAllowOutOfOrder, ksmStickyAllowOutOfOrder, errorMessageSubscriptionModeAutoSplit },
                { ksmAutoSplitAllowOutOfOrder, ksmAutoSplit, errorMessageOutOfOrderAllowed },

                { ksmSticky, ksmStickyAllowOutOfOrder, errorMessageOutOfOrderNotAllowed },
                { ksmSticky, ksmAutoSplit, errorMessageSubscriptionModeSticky },
                { ksmSticky, ksmAutoSplitAllowOutOfOrder, errorMessageSubscriptionModeSticky },

                { ksmStickyAllowOutOfOrder, ksmSticky, errorMessageOutOfOrderAllowed },
                { ksmStickyAllowOutOfOrder, ksmAutoSplit, errorMessageSubscriptionModeSticky },
                { ksmStickyAllowOutOfOrder, ksmAutoSplitAllowOutOfOrder, errorMessageSubscriptionModeSticky }
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