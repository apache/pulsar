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
package org.apache.pulsar.packages.management.core;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import lombok.Cleanup;
import org.testng.annotations.Test;

public class MockedPackagesStorageTest {

    @Test
    public void testWriteAndRead() throws Exception {
        PackagesStorageProvider provider = new MockedPackagesStorageProvider();
        PackagesStorage storage = provider.getStorage(mock(PackagesStorageConfiguration.class));
        storage.initialize();

        // Test data
        byte[] testBytes = new byte[1 * 1024 * 1024];

        // Write
        storage.writeAsync("test/path", new ByteArrayInputStream(testBytes)).get();

        // Read
        @Cleanup
        ByteArrayOutputStream readBaos = new ByteArrayOutputStream();
        storage.readAsync("test/path", readBaos).get();

        // Verify
        assertEquals(readBaos.toByteArray(), testBytes);

        storage.closeAsync().get();
    }
}
