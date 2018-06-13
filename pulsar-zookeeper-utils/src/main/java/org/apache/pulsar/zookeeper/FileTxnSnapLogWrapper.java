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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

@Slf4j
public class FileTxnSnapLogWrapper extends FileTxnSnapLog {

    public FileTxnSnapLogWrapper(FileTxnSnapLog src) throws IOException {
        this(src.getDataDir(), src.getSnapDir());
    }

    public FileTxnSnapLogWrapper(File dataDir, File snapDir) throws IOException {
        super(dataDir, snapDir);
    }

    @Override
    public long restore(DataTree dt, Map<Long, Integer> sessions, PlayBackListener listener) throws IOException {
        try {
            return super.restore(dt, sessions, listener);
        } catch (IOException e) {
            if ("No snapshot found, but there are log entries. Something is broken!".equals(e.getMessage())) {
                log.info("Ignoring exception for missing ZK db");
                // Ignore error when snapshot is not found. This is needed when upgrading ZK from 3.4 to 3.5
                // https://issues.apache.org/jira/browse/ZOOKEEPER-3056
                save(dt, (ConcurrentHashMap<Long, Integer>) sessions);

                /* return a zxid of zero, since we the database is empty */
                return 0;
            } else {
                throw e;
            }
        }
    }
}
