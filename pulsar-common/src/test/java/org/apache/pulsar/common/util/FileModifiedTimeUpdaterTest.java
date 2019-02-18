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

package org.apache.pulsar.common.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FileModifiedTimeUpdaterTest {
    @DataProvider(name = "files")
    Object[] getFiles() {
        return new Object[] { "/tmp/file.ini", "/tmp/file.log", "/tmp/f3/notes.txt" };
    }

    @Test(dataProvider = "files")
    public void testFileModified(String fileName) throws IOException, InterruptedException {
        Path path = Paths.get(fileName);
        createFile(path);
        FileModifiedTimeUpdater fileModifiedTimeUpdater = new FileModifiedTimeUpdater(fileName);
        Thread.sleep(2000);
        Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis()));
        FileTime fileTime = fileModifiedTimeUpdater.getLastModifiedTime();
        Assert.assertTrue(fileModifiedTimeUpdater.checkAndRefresh());
        Assert.assertNotEquals(fileTime, fileModifiedTimeUpdater.getLastModifiedTime());
    }

    public void createFile(Path path) throws IOException {
        if (Files.notExists(path)) {
            if (!Files.exists(path.getParent())) {
                Files.createDirectories(path.getParent());
            }
            path.toFile().createNewFile();
        }
    }

    @Test(dataProvider = "files")
    public void testFileNotModified(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        createFile(path);
        FileModifiedTimeUpdater fileModifiedTimeUpdater = new FileModifiedTimeUpdater(fileName);
        FileTime fileTime = fileModifiedTimeUpdater.getLastModifiedTime();
        Assert.assertFalse(fileModifiedTimeUpdater.checkAndRefresh());
        Assert.assertEquals(fileTime, fileModifiedTimeUpdater.getLastModifiedTime());
    }
}
