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
package org.apache.pulsar.packages.management.core;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.IOException;

/**
 * The provider provides a generic method to get a storage provider.
 */
public interface PackagesStorageProvider {
    /**
     * Construct a provider from the provided class.
     *
     * @param providerClassName the provider class name
     * @return an instance of package storage provider
     * @throws IOException
     */
    static PackagesStorageProvider newProvider(String providerClassName) throws IOException {
        Class<?> providerClass;
        try {
            providerClass = Class.forName(providerClassName);
            Object obj = providerClass.getDeclaredConstructor().newInstance();
            checkArgument(obj instanceof PackagesStorageProvider,
                "The package storage provider has to be an instance of " + PackagesStorageProvider.class.getName());
            return (PackagesStorageProvider) obj;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Get a storage with the configuration.
     *
     * @param config storage configuration
     * @return
     */
    PackagesStorage getStorage(PackagesStorageConfiguration config);
}
