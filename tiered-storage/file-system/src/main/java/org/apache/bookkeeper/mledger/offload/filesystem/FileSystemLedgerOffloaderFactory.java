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

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.offload.filesystem.impl.FileSystemManagedLedgerOffloader;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;

import java.io.IOException;
import java.util.Map;

public class FileSystemLedgerOffloaderFactory implements LedgerOffloaderFactory<FileSystemManagedLedgerOffloader> {
    @Override
    public boolean isDriverSupported(String driverName) {
        return FileSystemManagedLedgerOffloader.driverSupported(driverName);
    }

    @Override
    public FileSystemManagedLedgerOffloader create(OffloadPoliciesImpl offloadPolicies,
                                                   Map<String, String> userMetadata, OrderedScheduler scheduler) throws IOException {
        return FileSystemManagedLedgerOffloader.create(offloadPolicies, scheduler);
    }
}
