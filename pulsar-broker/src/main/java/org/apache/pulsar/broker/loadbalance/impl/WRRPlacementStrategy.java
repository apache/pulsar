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
package org.apache.pulsar.broker.loadbalance.impl;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import java.util.Map;
import java.util.Random;
import org.apache.pulsar.broker.loadbalance.PlacementStrategy;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * This class implements PlacementStrategy based on Weighted Round Robin Algorithm.
 */
public class WRRPlacementStrategy implements PlacementStrategy {
    private static final Logger log = LoggerFactory.getLogger(WRRPlacementStrategy.class);
    private final Random rand = new Random();

    /**
     * Function : getByWeightedRoundRobin returns ResourceUnit selected by WRR algorithm
     *              based on available resource on RU.
     * <code>
     * ^
     * |
     * |
     * |
     * |                |                        |                                |     |
     * |                |                        |                                |     |
     * |   Broker 2     |       Broker 3         |         Broker 1               |  B4 |
     * |                |                        |                                |     |
     * +----------------+------------------------+--------------------------------+--------->
     * 0                20                       50                               90    100
     *
     * This is weighted Round robin, we calculate weight based on availability of resources;
     * total availability is taken as a full range then each broker is given range based on
     *  its resource availability, if the number generated within total range happens to be in
     * broker's range, that broker is selected
     * </code>
     */
    public ResourceUnit findBrokerForPlacement(Multimap<Long, ResourceUnit> finalCandidates) {
        if (finalCandidates.isEmpty()) {
            return null;
        }
        log.debug("Total Final Candidates selected - [{}]", finalCandidates.size());
        int totalAvailability = 0;
        for (Map.Entry<Long, ResourceUnit> candidateOwner : finalCandidates.entries()) {
            totalAvailability += candidateOwner.getKey().intValue();
        }
        ResourceUnit selectedRU = null;
        if (totalAvailability <= 0) {
            // todo: this means all the brokers are overloaded and we can't assign this namespace to any broker
            // for now, pick anyone and return that one, because when we don't have ranking we put O for each broker
            return Iterables.get(finalCandidates.get(0L), rand.nextInt(finalCandidates.size()));
        }
        int weightedSelector = rand.nextInt(totalAvailability);
        log.debug("Generated Weighted Selector Number - [{}] ", weightedSelector);
        int weightRangeSoFar = 0;
        for (Map.Entry<Long, ResourceUnit> candidateOwner : finalCandidates.entries()) {
            weightRangeSoFar += candidateOwner.getKey();
            if (weightedSelector < weightRangeSoFar) {
                selectedRU = candidateOwner.getValue();
                log.debug(" Weighted Round Robin Selected RU - [{}]", candidateOwner.getValue().getResourceId());
                break;
            }
        }
        return selectedRU;
    }
}
