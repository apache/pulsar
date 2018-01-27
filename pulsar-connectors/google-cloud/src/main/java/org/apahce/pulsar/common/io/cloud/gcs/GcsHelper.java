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
package org.apahce.pulsar.common.io.cloud.gcs;


import com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageWriteChannel;
import com.google.cloud.hadoop.gcsio.ObjectWriteConditions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class GcsHelper {

    private final Storage storageClient;

    private final ExecutorService executorService;

    private GcsHelper(Storage storageClient, ExecutorService executorService) {
        this.storageClient = storageClient;
        this.executorService = executorService;
    }

    WritableByteChannel create(GcsPath path) throws IOException {
        final GoogleCloudStorageWriteChannel channel = new GoogleCloudStorageWriteChannel(
                executorService,
                storageClient,
                new ClientRequestHelper<>(),
                path.getBucket(),
                path.getObject(),
                AsyncWriteChannelOptions.newBuilder().build(),
                new ObjectWriteConditions(),
                Collections.emptyMap());

        channel.initialize();

        return channel;
    }

    static GcsHelper create(Storage storage) {
        return new GcsHelper(storage, Executors.newSingleThreadExecutor());
    }
}
