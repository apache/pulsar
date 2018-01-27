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
package org.apache.pulsar.common.io.fs;

// code based on apache beam io
public interface ResourceId {

    /**
     * Get the scheme which defines the namespace of the {@link ResourceId}.
     *
     * <p>The scheme is required to follow URI scheme syntax. See
     * <a href="https://www.ietf.org/rfc/rfc2396.txt">RFC 2396</a>
     */
    String getScheme();


    /**
     * Returns the name of the file or directory denoted by this {@code ResourceId}. The file name
     * is the farthest element from the root in the directory hierarchy.
     *
     * @return a string representing the name of file or directory, or null if there are zero
     * components.
     */
    String getFilename();

    /**
     * Returns {@code true} if this {@link ResourceId} represents a directory, false otherwise.
     */
    boolean isDirectory();

    /**
     * Returns the string representation of this {@link ResourceId}.
     */
    String toString();
}
