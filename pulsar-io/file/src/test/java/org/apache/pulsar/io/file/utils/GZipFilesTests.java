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
package org.apache.pulsar.io.file.utils;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.testng.annotations.Test;

public class GZipFilesTests {

    @Test
    public final void validGzipFileTest() {
        assertTrue(GZipFiles.isGzip(getFile("org/apache/pulsar/io/file/validGzip.gz")));
    }
    
    @Test
    public final void nonGzipFileTest() {
        assertFalse(GZipFiles.isGzip(getFile("org/apache/pulsar/io/file/nonGzipFile.txt")));
    }
    
    @Test
    public final void mislabelledGzipFileTest() {
        assertFalse(GZipFiles.isGzip(getFile("org/apache/pulsar/io/file/mislabelled.gz")));
    }
    
    @Test
    public final void nonExistantGzipFileTest() {
        assertFalse(GZipFiles.isGzip(null));
    }
    
    @Test
    public final void streamGzipFileTest() {
        Path path = Paths.get(getFile("org/apache/pulsar/io/file/validGzip.gz").getAbsolutePath(), "");
        
        try (Stream<String> lines = GZipFiles.lines(path)) {
            lines.forEachOrdered(line -> assertTrue(line.startsWith("Line ")));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
