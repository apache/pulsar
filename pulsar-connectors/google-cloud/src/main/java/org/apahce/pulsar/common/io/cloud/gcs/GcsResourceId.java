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

import org.apache.pulsar.common.io.fs.ResourceId;

import static com.google.api.client.util.Preconditions.checkNotNull;

// code based on apache beam io
class GcsResourceId implements ResourceId {

    private final GcsPath gcsPath;

    static GcsResourceId fromGcsPath(GcsPath gcsPath) {
        checkNotNull(gcsPath, "gcsPath");
        return new GcsResourceId(gcsPath);
    }

    private GcsResourceId(GcsPath gcsPath) {
        this.gcsPath = gcsPath;
    }

    @Override
    public boolean isDirectory() {
        return gcsPath.endsWith("/");
    }

    @Override
    public String getScheme() {
        return "gs";
    }

    @Override
    public String getFilename() {
        if (gcsPath.getNameCount() <= 1) {
            return null;
        } else {
            GcsPath gcsFilename = gcsPath.getFileName();
            return gcsFilename == null ? null : gcsFilename.toString();
        }
    }

    GcsPath getGcsPath() {
        return gcsPath;
    }

    @Override
    public String toString() {
        return gcsPath.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GcsResourceId)) {
            return false;
        }
        GcsResourceId other = (GcsResourceId) obj;
        return this.gcsPath.equals(other.gcsPath);
    }

    @Override
    public int hashCode() {
        return gcsPath.hashCode();
    }
}
