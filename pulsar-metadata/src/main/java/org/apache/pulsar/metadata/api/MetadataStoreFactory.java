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
package org.apache.pulsar.metadata.api;

import java.io.IOException;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;

/**
 * Factory class for {@link MetadataStore}.
 */
@UtilityClass
public class MetadataStoreFactory {
    /**
     * Create a new {@link MetadataStore} instance based on the given configuration.
     *
     * @param metadataURL
     *            the metadataStore URL
     * @param metadataStoreConfig
     *            the configuration object
     * @return a new {@link MetadataStore} instance
     * @throws IOException
     *             if the metadata store initialization fails
     */
    public static MetadataStore create(String metadataURL, MetadataStoreConfig metadataStoreConfig)
            throws MetadataStoreException {
        return MetadataStoreFactoryImpl.create(metadataURL, metadataStoreConfig);
    }
}
