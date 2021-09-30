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
package org.apache.pulsar.common.policies.data;

import static org.apache.pulsar.common.policies.data.Policies.FIRST_BOUNDARY;
import static org.apache.pulsar.common.policies.data.Policies.LAST_BOUNDARY;
import org.apache.pulsar.common.util.RestException;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils of Pulsar policies.
 */
public class PoliciesUtil {

    public static BundlesData defaultBundle() {
        List<String> boundaries = new ArrayList<>();
        boundaries.add(FIRST_BOUNDARY);
        boundaries.add(LAST_BOUNDARY);
        return BundlesData.builder()
                .numBundles(1)
                .boundaries(boundaries)
                .build();
    }

    public static void setStorageQuota(Policies polices, BacklogQuota quota) {
        if (polices == null) {
            return;
        }
        polices.backlog_quota_map.put(BacklogQuota.BacklogQuotaType.destination_storage, quota);
    }

    private static final long MAX_BUNDLES = ((long) 1) << 32;

    public static BundlesData getBundles(int numBundles) {
        if (numBundles <= 0) {
            throw new RestException(Response.Status.BAD_REQUEST,
                    "Invalid number of bundles. Number of bundles has to be in the range of (0, 2^32].");
        }
        Long maxVal = MAX_BUNDLES;
        Long segSize = maxVal / numBundles;
        List<String> partitions = new ArrayList<>();
        partitions.add(String.format("0x%08x", 0L));
        Long curPartition = segSize;
        for (int i = 0; i < numBundles; i++) {
            if (i != numBundles - 1) {
                partitions.add(String.format("0x%08x", curPartition));
            } else {
                partitions.add(String.format("0x%08x", maxVal - 1));
            }
            curPartition += segSize;
        }
        return BundlesData.builder()
                .boundaries(partitions)
                .numBundles(numBundles)
                .build();
    }
}
