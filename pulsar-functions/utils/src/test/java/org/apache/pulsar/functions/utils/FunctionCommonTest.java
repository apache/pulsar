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

import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link Exceptions}.
 */
public class FunctionCommonTest {

    @Test
    public void testValidateLocalFileUrl() throws Exception {
        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        try {
            // eg: fileLocation : /dir/fileName.jar (invalid)
            FunctionCommon.extractClassLoader(fileLocation);
            Assert.fail("should fail with invalid url: without protocol");
        } catch (IllegalArgumentException ie) {
            // Ok.. expected exception
        }
        String fileLocationWithProtocol = "file://" + fileLocation;
        // eg: fileLocation : file:///dir/fileName.jar (valid)
        FunctionCommon.extractClassLoader(fileLocationWithProtocol);
        // eg: fileLocation : file:/dir/fileName.jar (valid)
        fileLocationWithProtocol = "file:" + fileLocation;
        FunctionCommon.extractClassLoader(fileLocationWithProtocol);
    }

    @Test
    public void testValidateHttpFileUrl() throws Exception {

        String jarHttpUrl = "https://repo1.maven.org/maven2/org/apache/pulsar/pulsar-common/2.4.2/pulsar-common-2.4.2.jar";
        FunctionCommon.extractClassLoader(jarHttpUrl);

        jarHttpUrl = "http://_invalidurl_.com";
        try {
            // eg: fileLocation : /dir/fileName.jar (invalid)
            FunctionCommon.extractClassLoader(jarHttpUrl);
            Assert.fail("should fail with invalid url: without protocol");
        } catch (Exception ie) {
            // Ok.. expected exception
        }
    }

    @Test
    public void testDownloadFile() throws Exception {
        String jarHttpUrl = "https://repo1.maven.org/maven2/org/apache/pulsar/pulsar-common/2.4.2/pulsar-common-2.4.2.jar";
        String testDir = FunctionCommonTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        File pkgFile = new File(testDir, UUID.randomUUID().toString());
        FunctionCommon.downloadFromHttpUrl(jarHttpUrl, pkgFile);
        Assert.assertTrue(pkgFile.exists());
        pkgFile.delete();
    }

    @Test
    public void testGetSequenceId() {
        long lid = 12345L;
        long eid = 34566L;
        MessageIdImpl id = mock(MessageIdImpl.class);
        when(id.getLedgerId()).thenReturn(lid);
        when(id.getEntryId()).thenReturn(eid);

        assertEquals((lid << 28) | eid, FunctionCommon.getSequenceId(id));
    }

    @Test
    public void testGetMessageId() {
        long lid = 12345L;
        long eid = 34566L;
        long sequenceId = (lid << 28) | eid;

        MessageIdImpl id = (MessageIdImpl) FunctionCommon.getMessageId(sequenceId);
        assertEquals(lid, id.getLedgerId());
        assertEquals(eid, id.getEntryId());
    }
}
