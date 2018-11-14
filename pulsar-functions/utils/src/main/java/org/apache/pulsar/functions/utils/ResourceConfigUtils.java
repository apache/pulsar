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

package org.apache.pulsar.functions.utils;

import org.apache.pulsar.common.functions.Resources;

public class ResourceConfigUtils {
    public static void validate(Resources resources) {
        Double cpu = resources.getCpu();
        Long ram = resources.getRam();
        Long disk = resources.getDisk();
        com.google.common.base.Preconditions.checkArgument(cpu == null || cpu > 0.0,
                "The cpu allocation for the function must be positive");
        com.google.common.base.Preconditions.checkArgument(ram == null || ram > 0L,
                "The ram allocation for the function must be positive");
        com.google.common.base.Preconditions.checkArgument(disk == null || disk > 0L,
                "The disk allocation for the function must be positive");
    }

    public static Resources merge(Resources existingResources, Resources newResources) {
        Resources mergedResources;
        if (existingResources != null) {
            mergedResources = existingResources.toBuilder().build();
        } else {
            mergedResources = new Resources();
        }
        if (newResources.getCpu() != null) {
            mergedResources.setCpu(newResources.getCpu());
        }if (newResources.getRam() != null) {
            mergedResources.setRam(newResources.getRam());
        }
        if (newResources.getDisk() != null) {
            mergedResources.setDisk(newResources.getDisk());
        }
        return mergedResources;
    }
}