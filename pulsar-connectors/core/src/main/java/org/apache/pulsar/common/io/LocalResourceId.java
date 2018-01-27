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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

// code based on apache beam io
class LocalResourceId implements ResourceId {

    private final String pathString;
    private transient volatile Path cachedPath;
    private final boolean isDirectory;

    static LocalResourceId fromPath(Path path, boolean isDirectory) {
        //checkNotNull(path, "path");
        return new LocalResourceId(path, isDirectory);
    }

    private LocalResourceId(Path path, boolean isDirectory) {
        this.pathString = path.toAbsolutePath().normalize().toString()
                + (isDirectory ? File.separatorChar : "");
        this.isDirectory = isDirectory;
    }

    @Override
    public String getFilename() {
        Path fileName = getPath().getFileName();
        return fileName == null ? null : fileName.toString();
    }

    @Override
    public String getScheme() {
        return "file";
    }

    @Override
    public boolean isDirectory() {
        return isDirectory;
    }

    Path getPath() {
        if (cachedPath == null) {
            cachedPath = Paths.get(pathString);
        }
        return cachedPath;
    }

    @Override
    public String toString() {
        return pathString;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LocalResourceId)) {
            return false;
        }
        LocalResourceId other = (LocalResourceId) obj;
        return this.pathString.equals(other.pathString)
                && this.isDirectory == other.isDirectory;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathString, isDirectory);
    }
}
