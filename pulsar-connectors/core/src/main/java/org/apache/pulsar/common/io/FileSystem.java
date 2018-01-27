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
package org.apache.pulsar.common.io;

import org.apache.pulsar.common.io.fs.ResourceId;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

// code based on apache beam io
public abstract class FileSystem<ResourceIdT extends ResourceId> {

    protected abstract WritableByteChannel create(
            ResourceIdT resourceId) throws IOException;

    /**
     * Returns a new {@link ResourceId} for this filesystem that represents the named resource.
     * The user supplies both the resource spec and whether it is a directory.
     *
     * <p>The supplied {@code singleResourceSpec} is expected to be in a proper format, including
     * any necessary escaping, for this {@link FileSystem}.
     *
     * <p>This function may throw an {@link IllegalArgumentException} if given an invalid argument,
     * such as when the specified {@code singleResourceSpec} is not a valid resource name.
     */
    protected abstract ResourceIdT matchNewResource(String singleResourceSpec, boolean isDirectory);

    /**
     * Get the URI scheme which defines the namespace of the {@link FileSystem}.
     *
     * @see <a href="https://www.ietf.org/rfc/rfc2396.txt">RFC 2396</a>
     */
    protected abstract String getScheme();
}
