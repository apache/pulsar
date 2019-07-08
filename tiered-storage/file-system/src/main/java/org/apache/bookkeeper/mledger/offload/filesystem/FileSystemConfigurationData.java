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
package org.apache.bookkeeper.mledger.offload.filesystem;

import lombok.Data;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.pulsar.common.util.FieldParser.value;

/**
 * Configuration for file system.
 */
@Data
public class FileSystemConfigurationData implements Serializable, Cloneable {

    /**** --- Ledger Offloading --- ****/
    // Driver to use to offload old data to long term storage
    private String managedLedgerOffloadDriver = null;

    private String fileSystemProfilePath = null;

    private String fileSystemURI = null;

    private int managedLedgerOffloadMaxThreads = 2;

    /**
     * Create a tiered storage configuration from the provided <tt>properties</tt>.
     *
     * @param properties the configuration properties
     * @return tiered storage configuration
     */
    public static FileSystemConfigurationData create(Properties properties) {
        FileSystemConfigurationData data = new FileSystemConfigurationData();
        Field[] fields = FileSystemConfigurationData.class.getDeclaredFields();
        Arrays.stream(fields).forEach(f -> {
            if (properties.containsKey(f.getName())) {
                try {
                    f.setAccessible(true);
                    f.set(data, value((String) properties.get(f.getName()), f));
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format("failed to initialize %s field while setting value %s",
                            f.getName(), properties.get(f.getName())), e);
                }
            }
        });
        return data;
    }
}
