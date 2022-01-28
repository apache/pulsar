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

import java.util.Comparator;
import java.util.Map;
import org.apache.pulsar.broker.loadbalance.LoadRanker;
import org.apache.pulsar.broker.loadbalance.ResourceDescription;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ResourceAvailabilityRanker implements LoadRanker, Comparator<ResourceDescription> {
    private static final Logger log = LoggerFactory.getLogger(ResourceAvailabilityRanker.class);

    /*
     * Every resource's percentage availability is calculated and then added up to denote the total availability of a
     * Resource Unit, if any available resource on a Resource unit is less than 5% we take it out, if only 20% of total
     * resources are available then the rank would be halved for every resource that's less than 20% available
     *
     * Since this rank is based on ResourceAvailability Higher Number Rank means more availability, which is quite
     * opposite of the word rank.
     */
    @Override
    public long getRank(ResourceDescription resourceDescription) {
        int weight = 1;
        int availabilityRank = 0;
        int resourcesWithLowAvailability = 0;
        // todo: need to re-think these numbers, keep it for now
        // any resource with only 20% availability of total capacity will broker's probability to be selected
        int minAvailableRequired = 20;
        // any resource available only 5% will take this machine out of active
        int absolutelyMinRequiredToFunction = 5;
        boolean makeNonFunctional = false;
        for (Map.Entry<String, ResourceUsage> entry : resourceDescription.getResourceUsage().entrySet()) {
            int percentAvailable = 0;
            if (entry.getValue().limit > 0 && entry.getValue().limit > entry.getValue().usage) {
                Double temp = ((entry.getValue().limit - entry.getValue().usage) / entry.getValue().limit) * 100;
                percentAvailable = temp.intValue();
            }
            log.debug("Resource [{}] in Percentage Available - [{}], Actual Usage is - [{}], Actual Limit is [{}]",
                    entry.getKey(), percentAvailable, entry.getValue().usage, entry.getValue().limit);
            // give equal weight to each resource
            int resourceWeight = weight * percentAvailable;
            if (percentAvailable < minAvailableRequired) {
                resourcesWithLowAvailability++;
            }
            availabilityRank += resourceWeight;
            if (percentAvailable < absolutelyMinRequiredToFunction) {
                makeNonFunctional = true;
            }
        }
        if (resourcesWithLowAvailability > 0) {
            availabilityRank = availabilityRank / (resourcesWithLowAvailability * 2);
            log.debug("Total Resource with Low availability - [{}]", resourcesWithLowAvailability);
        }
        if (makeNonFunctional) {
            // this will rarely be selected
            availabilityRank = 0;
            log.debug("ResourceUnit set to non-functional due to extremely low resources");
        }
        return availabilityRank;
    }

    public int compare(ResourceDescription rd1, ResourceDescription rd2) {
        return ((Long) getRank(rd1)).compareTo(getRank(rd2));
    }
}
