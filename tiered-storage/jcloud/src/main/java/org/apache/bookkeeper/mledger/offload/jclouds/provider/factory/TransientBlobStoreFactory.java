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
package org.apache.bookkeeper.mledger.offload.jclouds.provider.factory;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.TransientApiMetadata;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.providers.AnonymousProviderMetadata;
import org.jclouds.providers.ProviderMetadata;

/**
 * Configuration for Transient Blob storage.
 */
public class TransientBlobStoreFactory extends JCloudBlobStoreFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public void validate() {
        // No-op
    }

    @Override
    public ContextBuilder getContextBuilder() {
        return ContextBuilder.newBuilder(provider.getDriver());
    }

    @Override
    public BlobStore getBlobStore() {
        BlobStore bs = super.getBlobStore();
        if (!bs.containerExists(getBucket())) {
          Location loc = new LocationBuilder()
                  .scope(LocationScope.HOST)
                  .id("")
                  .description("")
                  .build();
          bs.createContainerInLocation(loc, getBucket());
        }
        return bs;
    }

    @Override
    public ProviderMetadata getProviderMetadata() {
        return new AnonymousProviderMetadata(new TransientApiMetadata(), "");
    }

}
