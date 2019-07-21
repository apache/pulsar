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
package org.apache.bookkeeper.mledger.offload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.nar.NarClassLoader;

@Slf4j
@Data
public class Offloaders implements AutoCloseable {

    private final List<Pair<NarClassLoader, LedgerOffloaderFactory>> offloaders = new ArrayList<>();

    public LedgerOffloaderFactory getOffloaderFactory(String driverName) throws IOException {
        for (Pair<NarClassLoader, LedgerOffloaderFactory> factory : offloaders) {
            if (factory.getRight().isDriverSupported(driverName)) {
                return factory.getRight();
            }
        }
        throw new IOException("No offloader found for driver '" + driverName + "'." +
            " Please make sure you dropped the offloader nar packages under `${PULSAR_HOME}/offloaders`.");
    }

    @Override
    public void close() throws Exception {
        offloaders.forEach(offloader -> {
            try {
                offloader.getLeft().close();
            } catch (IOException e) {
                log.warn("Failed to close nar class loader for offloader '{}': {}",
                    offloader.getRight().getClass(), e.getMessage());
            }
        });
    }
}
