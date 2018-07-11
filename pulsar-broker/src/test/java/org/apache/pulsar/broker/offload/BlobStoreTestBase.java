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
package org.apache.pulsar.broker.offload;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BlobStoreTestBase {
    private static final Logger log = LoggerFactory.getLogger(BlobStoreTestBase.class);

    public final static String BUCKET = "pulsar-unittest";

    protected BlobStoreContext context = null;
    protected BlobStore blobStore = null;

    @BeforeMethod
    public void start() throws Exception {
        context = ContextBuilder.newBuilder("transient").build(BlobStoreContext.class);
        blobStore = context.getBlobStore();
        boolean create = blobStore.createContainerInLocation(null, BUCKET);

        log.debug("TestBase Create Bucket: {}, in blobStore, result: {}", BUCKET, create);
    }

    @AfterMethod
    public void tearDown() {
        if (blobStore != null) {
            blobStore.deleteContainer(BUCKET);
        }

        if (context != null) {
            context.close();
        }
    }

}
