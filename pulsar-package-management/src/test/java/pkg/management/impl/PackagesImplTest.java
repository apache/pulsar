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
 *
 */

package pkg.management.impl;

import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.distributedlog.api.namespace.Namespace;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import pkg.management.PkgMetadata;
import pkg.management.PkgStorage;
import pkg.management.exception.PackageMetaNotFoundException;
import pkg.management.naming.PkgName;
import pkg.management.storage.bk.BKStorage;

public class PackagesImplTest {

    private PkgStorage storage;

    @BeforeMethod
    private void setupNsNotFound() throws IOException {
        Namespace namespace = mock(Namespace.class);
        this.storage = new BKStorage(namespace);
        when(namespace.logExists(anyString())).thenReturn(false);
    }

    @Test
    public void testPackageErrors() throws IOException {
        PkgName name = PkgName.get("function://public/default/not-found@v1");
        PackagesImpl packages = new PackagesImpl(this.storage);
        try {
            packages.getMeta(name).get();
            packages.updateMeta(name, new PkgMetadata());
            packages.download(name, new ByteArrayOutputStream());
            packages.delete(name);
            packages.list(name);
        } catch (Exception e) {
            if (e.getCause() instanceof PackageMetaNotFoundException) {
                // no-op
            } else {
                fail("Unexpect exception");
            }
        }
    }
}
