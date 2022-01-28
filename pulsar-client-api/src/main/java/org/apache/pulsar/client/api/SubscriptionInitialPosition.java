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
package org.apache.pulsar.client.api;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * When creating a consumer, if the subscription does not exist, a new subscription will be created. By default the
 * subscription will be created at the end of the topic. See
 * {@link #subscriptionInitialPosition(SubscriptionInitialPosition)} to configure the initial position behavior.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum SubscriptionInitialPosition {
    /**
     * The latest position which means the start consuming position will be the last message.
     */
    Latest(0),

    /**
     * The earliest position which means the start consuming position will be the first message.
     */
    Earliest(1);


    private final int value;

    SubscriptionInitialPosition(int value) {
        this.value = value;
    }

    public final int getValue() {
        return value;
    }

}
