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

import org.apache.pulsar.common.io.FileSystem;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

// code based on apache beam io
public class GcsFileSystem extends FileSystem<GcsResourceId> {

    private final GcsHelper helper;

    GcsFileSystem(GcsHelper helper) {
        this.helper = helper;
    }

    @Override
    protected WritableByteChannel create(GcsResourceId resourceId) throws IOException {
        // TODO check that bucket exists before writing
        return helper.create(resourceId.getGcsPath());
    }

    @Override
    protected GcsResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
        if (isDirectory) {
            if (!singleResourceSpec.endsWith("/")) {
                singleResourceSpec += '/';
            }
        } else {
            if (singleResourceSpec.endsWith("/")) {
                throw new IllegalStateException(
                        String.format("Expected a file path, but [%s], ends with '/'. " +
                                "This is unsupported in GcsFileSystem.", singleResourceSpec)
                );
            }
        }
        GcsPath path = GcsPath.fromUri(singleResourceSpec);
        return GcsResourceId.fromGcsPath(path);
    }

    @Override
    protected String getScheme() {
        return "gs";
    }
}
