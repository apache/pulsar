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

package org.apache.pulsar.functions.utils;

import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test of {@link Exceptions}.
 */
public class UtilsTest {

    @Test
    public void testValidateLocalFileUrl() throws Exception {
        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        try {
            // eg: fileLocation : /dir/fileName.jar (invalid)
            Utils.extractClassLoader(fileLocation);
            Assert.fail("should fail with invalid url: without protocol");
        } catch (IllegalArgumentException ie) {
            // Ok.. expected exception
        }
        String fileLocationWithProtocol = "file://" + fileLocation;
        // eg: fileLocation : file:///dir/fileName.jar (valid)
        Utils.extractClassLoader(fileLocationWithProtocol);
        // eg: fileLocation : file:/dir/fileName.jar (valid)
        fileLocationWithProtocol = "file:" + fileLocation;
        Utils.extractClassLoader(fileLocationWithProtocol);
    }

    @Test
    public void testValidateHttpFileUrl() throws Exception {

        String jarHttpUrl = "http://central.maven.org/maven2/org/apache/pulsar/pulsar-common/1.22.0-incubating/pulsar-common-1.22.0-incubating.jar";
        Utils.extractClassLoader(jarHttpUrl);

        jarHttpUrl = "http://_invalidurl_.com";
        try {
            // eg: fileLocation : /dir/fileName.jar (invalid)
            Utils.extractClassLoader(jarHttpUrl);
            Assert.fail("should fail with invalid url: without protocol");
        } catch (Exception ie) {
            // Ok.. expected exception
        }
    }
}
