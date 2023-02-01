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
package org.apache.pulsar.broker.loadbalance.extensions.strategy;

import java.util.Optional;
import java.util.Set;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.common.naming.ServiceUnitId;

/**
 * The broker selection strategy is designed to select the broker according to different implementations.
 */
public interface BrokerSelectionStrategy {

    /**
     * Choose an appropriate broker according to different load balancing implementations.
     *
     * @param brokers
     *               The candidate brokers list.
     * @param bundle
     *               The input bundle to select the owner broker
     * @param context
     *               The context contains information needed for selection (load data, config, and etc).
     */
    Optional<String> select(Set<String> brokers, ServiceUnitId bundle, LoadManagerContext context);

}
