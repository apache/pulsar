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
package org.apache.pulsar.common.policies.data.impl;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.policies.data.BundlesData;

/**
 * Holder for bundles.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class BundlesDataImpl implements BundlesData {
    private List<String> boundaries;
    private int numBundles;

    public static BundlesDataImplBuilder builder() {
        return new BundlesDataImplBuilder();
    }

    public static class BundlesDataImplBuilder implements BundlesData.Builder {
        private List<String> boundaries;
        private int numBundles = 0;

        public BundlesDataImplBuilder boundaries(List<String> boundaries) {
            this.boundaries = boundaries;
            return this;
        }

        public BundlesDataImplBuilder numBundles(int numBundles) {
            this.numBundles = numBundles;
            return this;
        }

        public BundlesDataImpl build() {
            return new BundlesDataImpl(boundaries, numBundles);
        }
    }
}
