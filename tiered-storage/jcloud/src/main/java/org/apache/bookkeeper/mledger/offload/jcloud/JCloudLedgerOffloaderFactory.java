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
package org.apache.bookkeeper.mledger.offload.jcloud;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.BlobStoreManagedLedgerOffloader;

/**
 * A jcloud based offloader factory.
 */
public class JCloudLedgerOffloaderFactory implements LedgerOffloaderFactory<BlobStoreManagedLedgerOffloader> {

    public static JCloudLedgerOffloaderFactory of() {
        return INSTANCE;
    }

    private static final JCloudLedgerOffloaderFactory INSTANCE = new JCloudLedgerOffloaderFactory();

    @Override
    public boolean isDriverSupported(String driverName) {
        return BlobStoreManagedLedgerOffloader.driverSupported(driverName);
    }

    @Override
    public BlobStoreManagedLedgerOffloader create(Properties properties,
                                                  Map<String, String> userMetadata,
                                                  OrderedScheduler scheduler) throws IOException  {
        TieredStorageConfigurationData data = TieredStorageConfigurationData.create(properties);
        return BlobStoreManagedLedgerOffloader.create(data, userMetadata, scheduler);
    }
}
