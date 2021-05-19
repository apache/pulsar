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
package org.apache.pulsar.common.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import lombok.Getter;
import lombok.ToString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class working with file's modified time.
 */
@ToString
public class FileModifiedTimeUpdater {
    @Getter
    String fileName;
    @Getter
    FileTime lastModifiedTime;

    public FileModifiedTimeUpdater(String fileName) {
        this.fileName = fileName;
        this.lastModifiedTime = updateLastModifiedTime();
    }

    private FileTime updateLastModifiedTime() {
        if (fileName != null) {
            Path p = Paths.get(fileName);
            try {
                return Files.getLastModifiedTime(p);
            } catch (IOException e) {
                LOG.error("Unable to fetch lastModified time for file {}: ", fileName, e);
            }
        }
        return null;
    }

    public boolean checkAndRefresh() {
        FileTime newLastModifiedTime = updateLastModifiedTime();
        if (newLastModifiedTime != null && !newLastModifiedTime.equals(lastModifiedTime)) {
            this.lastModifiedTime = newLastModifiedTime;
            return true;
        }
        return false;
    }

    private static final Logger LOG = LoggerFactory.getLogger(FileModifiedTimeUpdater.class);
}
