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
package org.apache.pulsar.broker.loadbalance.extensions.filter;

import java.util.Map;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;

/**
 * Filter out unqualified Brokers, which are not entered into LoadBalancer for decision-making.
 */
public interface BrokerFilter {

    /**
     * The broker filter name.
     */
    String name();

    /**
     * Filter out unqualified brokers based on implementation.
     *
     * @param brokers The full broker and lookup data.
     * @param context The load manager context.
     * @return Filtered broker list.
     */
    Map<String, BrokerLookupData> filter(Map<String, BrokerLookupData> brokers, LoadManagerContext context)
            throws BrokerFilterException;

}
