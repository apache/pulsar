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
package org.apache.pulsar.io.file;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.Data;

import org.apache.pulsar.functions.api.Record;

/**
 * Implementation of the Record interface for File Source data.
 *    - The key is set to the source file name + the line number of the record.
 *    - The value is set to the file contents for the given line number (in bytes)
 *    - The following user properties are also set:
 *      - The source file name
 *      - The absolute path of the source file
 *      - The last modified time of the source file.
 */
@Data
public class FileRecord implements Record<byte[]> {

    public static final String FILE_NAME = "file.name";
    public static final String FILE_ABSOLUTE_PATH = "file.path";
    public static final String FILE_MODIFIED_TIME = "file.modified.time";

    private final Optional<String> key;
    private final byte[] value;
    private final HashMap<String, String> userProperties = new HashMap<String, String> ();

    public FileRecord(File srcFile, int lineNumber, byte[] value) {
        this.key = Optional.of(srcFile.getName() + "_" + lineNumber);
        this.value = value;
        this.setProperty(FILE_NAME, srcFile.getName());
        this.setProperty(FILE_ABSOLUTE_PATH, srcFile.getAbsolutePath());
        this.setProperty(FILE_MODIFIED_TIME, srcFile.lastModified() + "");
    }

    @Override
    public Optional<String> getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    public Map<String, String> getProperties() {
        return userProperties;
    }

    public void setProperty(String key, String value) {
        userProperties.put(key, value);
    }
}
