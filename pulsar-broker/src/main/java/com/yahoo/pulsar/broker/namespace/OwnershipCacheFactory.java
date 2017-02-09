/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.namespace;

import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;

public interface OwnershipCacheFactory {

    NamespaceOwnershipCache create(PulsarService pulsar, NamespaceBundleFactory bundleFactory);

    public static class OwnershipDualCacheFactoryImpl implements OwnershipCacheFactory {

        @Override
        public NamespaceOwnershipCache create(PulsarService pulsar, NamespaceBundleFactory bundleFactory) {
            return new DualOwnershipCache(pulsar, bundleFactory);
        }
        
    }
    
}
