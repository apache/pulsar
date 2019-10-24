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
package org.apache.pulsar.broker.namespace;

import org.apache.pulsar.common.naming.TopicName;

import java.util.function.Predicate;

/**
 * Listener for <code>Topic</code> ownership changes
 */
public interface TopicOwnershipListener {

    /**
     * Will be call after a <code>Topic</code> owned by broker
     * @param topic owned topic
     */
    void onLoad(TopicName topic);

    /**
     * Will be call after a <code>Topic</code> unloaded from broker
     * @param topic owned topic
     */
    void unLoad(TopicName topic);

    default Predicate<TopicName> getFilter() {
        return null;
    }
}
