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

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.broker.loadbalance.ResourceDescription;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;

public class PulsarResourceDescription extends ResourceDescription {

    // entry example : "cpu"->ResourceUsage(usage=80, limit=100)
    private Map<String, ResourceUsage> resourceUsageByName;

    public PulsarResourceDescription() {
        resourceUsageByName = new HashMap<String, ResourceUsage>();
    }

    @Override
    public int compareTo(ResourceDescription o) {
        if (o.getResourceUsage().size() > resourceUsageByName.size()) {
            return -1;
        }
        // TODO need to return zero if two resourceDescription match exactly
        for (Map.Entry<String, ResourceUsage> entry : o.getResourceUsage().entrySet()) {
            // if we don't have any entry which is in other but not in our set, we fail
            String resourceName = entry.getKey();
            // check if we have this resource, if not clearly we are lesser
            if (resourceUsageByName.containsKey(resourceName)) {
                int less = resourceUsageByName.get(resourceName).compareTo(entry.getValue());
                // not using the resource till its last shred, so <= 0 is failure
                if (less <= 0) {
                    return -1;
                }
            } else {
                return -1;
            }
        }
        return 1;
    }

    @Override
    public void removeUsage(ResourceDescription rd) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addUsage(ResourceDescription rd) {
        // TODO Auto-generated method stub

    }

    @Override
    public int getUsagePct() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Map<String, ResourceUsage> getResourceUsage() {
        return resourceUsageByName;
    }

    public void put(String resourceType, ResourceUsage resourceUsage) {
        resourceUsageByName.put(resourceType, resourceUsage);
    }

    public long calculateRank() {
        float weight = 1;
        if (resourceUsageByName.size() > 1) {
            weight = 1.0f / resourceUsageByName.size();
        }
        long rank = 0;
        int resourcesWithHighUsage = 0;
        int throttle = 75;
        for (Map.Entry<String, ResourceUsage> entry : resourceUsageByName.entrySet()) {
            double percentageUsage = 0;
            if (entry.getValue().limit > 0) {
                percentageUsage = (entry.getValue().usage / entry.getValue().limit) * 100;
            }
            // give equal weight to each resource
            double resourceWeight = weight * percentageUsage;
            // any resource usage over 75% doubles the whole weight per resource
            if (percentageUsage > throttle) {
                final int i = resourcesWithHighUsage++;
            }
            rank += resourceWeight;
        }
        if (resourcesWithHighUsage > 0) {
            rank = rank * resourcesWithHighUsage * 2;
        }
        return rank;
    }
}
