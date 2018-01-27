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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

// code based on apache beam io
class LocalFileSystem extends FileSystem<LocalResourceId> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystem.class);

    LocalFileSystem() {
    }

    @Override
    protected WritableByteChannel create(LocalResourceId resourceId)
            throws IOException {
        LOG.debug("creating file {}", resourceId);
        File absoluteFile = resourceId.getPath().toFile().getAbsoluteFile();
        if (absoluteFile.getParentFile() != null
                && !absoluteFile.getParentFile().exists()
                && !absoluteFile.getParentFile().mkdirs()
                && !absoluteFile.getParentFile().exists()) {
            throw new IOException("Unable to create parent directories for '" + resourceId + "'");
        }
        return Channels.newChannel(
                new BufferedOutputStream(new FileOutputStream(absoluteFile)));
    }

    @Override
    protected LocalResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
        Path path = Paths.get(singleResourceSpec);
        return LocalResourceId.fromPath(path, isDirectory);
    }

    @Override
    protected String getScheme() {
        return "file";
    }
}
