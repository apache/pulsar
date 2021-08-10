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
package org.apache.pulsar.zookeeper;

/**
 * ZooKeeper utils.
 */
public final class ZkUtils {

    /**
     * Returns the parent path of the provided path.
     * Works for UNIX-style paths only and is intended to be used for zookeeper paths.
     * If you have a file system path use {@link java.nio.file.Paths#get(String, String...)}
     * and {@link java.nio.file.Path#getParent()} instead.
     *
     * @param path the zookeeper path
     * @return the parent path or null if no parent exists
     */
    public static String getParentForPath(final String path) {
        if (path == null) {
            return null;
        }
        final int length = path.length();
        if (length == 0 || (length == 1 && path.charAt(0) == '/')) {
            return null;
        }

        int partStartIndex = 0;
        char lastChar = path.charAt(0);
        String lastPart = "/";  // needed, if it's an absolute path
        final StringBuilder sb = new StringBuilder();
        for (int index = 1; index < length; index++) {
            final char c = path.charAt(index);
            if (lastChar != '/') {
                if (c == '/') {
                    // First '/' after a non-'/' sequence defines the end of a part;
                    // save the part for later addition (when it's clear it is part
                    // of the parent path)
                    lastPart = path.substring(partStartIndex, index);
                    // Only needed, if it's the first part of a relative path to ensure
                    // that the next part includes the leading '/'
                    partStartIndex = -1;
                }
            } else if (c != '/') {
                // First non-'/' after a (series of) '/' indicates that there is a new part
                // after the saved, so the saved part definitely belongs to the parent path
                sb.append(lastPart);
                // Include the preceding '/' except for the first part
                partStartIndex = (partStartIndex == 0) ? index : index - 1;
            }
            lastChar = c;
        }
        final String parentPath = sb.toString();
        return (parentPath.length() == 0) ? null : parentPath;
    }
}
