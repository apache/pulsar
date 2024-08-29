/*
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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.Test;

@Slf4j
public class OxiaMetadataStoreErrorTest extends BaseMetadataStoreTest {

    @Test
    public void emptyStoreTest() throws Exception {
        String metadataStoreUrl = "oxia://" + getOxiaServerConnectString();
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(metadataStoreUrl,
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        oxiaServer.close();
        try {
            store.exists(prefix + "/non-existing-key").join();
            fail("Expected an exception because the metadata store server has been closed.");
        } catch (Exception ex) {
            Throwable actEx = FutureUtil.unwrapCompletionException(ex);
            assertTrue(actEx instanceof MetadataStoreException);
        }
    }
}
