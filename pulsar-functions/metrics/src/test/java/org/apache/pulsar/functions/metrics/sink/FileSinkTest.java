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
package org.apache.pulsar.functions.metrics.sink;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.fail;

/**
 * FileSink Tester.
 */
public class FileSinkTest {

    private FileSink fileSink;
    private File tmpDir;

    @BeforeMethod
    public void before() throws IOException {
        fileSink = new FileSink();
        Map<String, String> conf = new HashMap<>();
        tmpDir = Files.createTempDirectory("filesink").toFile();
        conf.put("filename-output", tmpDir.getAbsolutePath() + "/filesink");
        conf.put("file-maximum", "100");
        fileSink.init(conf);
    }

    @AfterMethod
    public void after() {
        fileSink.close();
        for (File file: tmpDir.listFiles()) {
            file.delete();
        }
        tmpDir.delete();
    }

    /**
     * Method: flush()
     */
    @Test
    public void testFirstFlushWithoutRecords() throws IOException {
        fileSink.flush();
        String content = new String(readFromFile(
                new File(tmpDir, "/filesink.0").getAbsolutePath()));
        assertEquals("[]", content);
    }

    /**
     * Method: flush()
     */
    @Test
    public void testSuccessiveFlushWithoutRecords() throws UnsupportedEncodingException, IOException {
        fileSink.flush();
        fileSink.flush();
        String content = new String(readFromFile(
                new File(tmpDir, "/filesink.0").getAbsolutePath()));
        assertEquals("[]", content);
        content = new String(readFromFile(new File(tmpDir, "/filesink.1").getAbsolutePath()));
        assertEquals("[]", content);
    }

    /**
     * Method: init()
     */
    @Test
    public void testIllegalConf() {
        FileSink sink = new FileSink();
        Map<String, String> conf = new HashMap<>();
        try {
            sink.init(conf);
            fail("Expected IllegalArgumentException.");
        } catch (IllegalArgumentException e) {
            assertEquals("Require: filename-output", e.getMessage());
        }

        sink = new FileSink();
        conf.put("filename-output", tmpDir.getAbsolutePath() + "/filesink");
        try {
            sink.init(conf);
            fail("Expected IllegalArgumentException.");
        } catch (IllegalArgumentException e) {
            assertEquals("Require: file-maximum", e.getMessage());
        }
    }

    private String readFromFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, StandardCharsets.UTF_8);
    }
}
