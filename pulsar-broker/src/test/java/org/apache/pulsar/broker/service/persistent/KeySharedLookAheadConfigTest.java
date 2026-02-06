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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers.getEffectiveLookAheadLimit;
import static org.testng.Assert.assertEquals;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

public class KeySharedLookAheadConfigTest {

    @Test
    public void testGetEffectiveLookAheadLimit() {
        ServiceConfiguration config = new ServiceConfiguration();

        config.setKeySharedLookAheadMsgInReplayThresholdPerConsumer(5);
        config.setKeySharedLookAheadMsgInReplayThresholdPerSubscription(100);
        assertEquals(getEffectiveLookAheadLimit(config, 5), 25);
        assertEquals(getEffectiveLookAheadLimit(config, 100), 100);

        config.setKeySharedLookAheadMsgInReplayThresholdPerConsumer(5);
        config.setKeySharedLookAheadMsgInReplayThresholdPerSubscription(0);
        assertEquals(getEffectiveLookAheadLimit(config, 100), 500);

        config.setKeySharedLookAheadMsgInReplayThresholdPerConsumer(0);
        config.setKeySharedLookAheadMsgInReplayThresholdPerSubscription(6000);
        assertEquals(getEffectiveLookAheadLimit(config, 100), 6000);

        config.setKeySharedLookAheadMsgInReplayThresholdPerConsumer(0);
        config.setKeySharedLookAheadMsgInReplayThresholdPerSubscription(0);
        config.setMaxUnackedMessagesPerConsumer(0);
        config.setMaxUnackedMessagesPerSubscription(0);
        assertEquals(getEffectiveLookAheadLimit(config, 100), Integer.MAX_VALUE);

        config.setKeySharedLookAheadMsgInReplayThresholdPerConsumer(0);
        config.setKeySharedLookAheadMsgInReplayThresholdPerSubscription(0);
        config.setMaxUnackedMessagesPerConsumer(1);
        config.setMaxUnackedMessagesPerSubscription(10);
        assertEquals(getEffectiveLookAheadLimit(config, 100), 10);

        config.setKeySharedLookAheadMsgInReplayThresholdPerConsumer(0);
        config.setKeySharedLookAheadMsgInReplayThresholdPerSubscription(0);
        config.setMaxUnackedMessagesPerConsumer(22);
        config.setMaxUnackedMessagesPerSubscription(0);
        assertEquals(getEffectiveLookAheadLimit(config, 100), 2200);
    }
}