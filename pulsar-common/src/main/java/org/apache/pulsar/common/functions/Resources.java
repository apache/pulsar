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
package org.apache.pulsar.common.functions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Class representing resources, such as CPU, RAM, and disk size.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class Resources {

    private static final Resources DEFAULT = new Resources();

    // Default cpu is 1 core
    private Double cpu = 1d;
    // Default memory is 1GB
    private Long ram = 1073741824L;
    // Default disk is 10GB
    private Long disk = 10737418240L;

    public static Resources getDefaultResources() {
        return DEFAULT;
    }

    public static Resources mergeWithDefault(Resources resources) {

        if (resources == null) {
            return DEFAULT;
        }

        double cpu = resources.getCpu() == null ? Resources.getDefaultResources().getCpu() : resources.getCpu();
        long ram = resources.getRam() == null ? Resources.getDefaultResources().getRam() : resources.getRam();
        long disk = resources.getDisk() == null ? Resources.getDefaultResources().getDisk() : resources.getDisk();

        return new Resources(cpu, ram, disk);
    }
}
