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
package org.apache.pulsar.functions.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test of {@link Utils}.
 */
public class UtilsTest {

    @Test
    public void testDownloadFile() throws Exception {
        String jarHttpUrl = "http://central.maven.org/maven2/org/apache/pulsar/pulsar-common/1.22.0-incubating/pulsar-common-1.22.0-incubating.jar";
        String testDir = UtilsTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        File pkgFile = new File(testDir, UUID.randomUUID().toString());
        Utils.downloadFromHttpUrl(jarHttpUrl, new FileOutputStream(pkgFile));
        Assert.assertTrue(pkgFile.exists());
        pkgFile.delete();
    }

}
