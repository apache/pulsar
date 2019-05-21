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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;

/**
 * Helper class that provides helper methods for working with
 * zip-formatted files.
 */
public class ZipFiles {

    /**
     * Returns true if the given file is a gzip file.
     */
    public static boolean isZip(File f) {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(f)))){
            int test = in.readInt();
            return test == 0x504b0304;
        } catch (final Exception e) {
            return false;
        }
    }

    /**
     * Get a lazily loaded stream of lines from a gzipped file, similar to
     * {@link Files#lines(java.nio.file.Path)}.
     *
     * @param path
     *          The path to the zipped file.
     * @return stream with lines.
     */
    public static Stream<String> lines(Path path) {
        ZipInputStream zipStream = null;

        try {
          zipStream = new ZipInputStream(Files.newInputStream(path));
        } catch (IOException e) {
          closeSafely(zipStream);
          throw new UncheckedIOException(e);
        }
        // Reader decoder = new InputStreamReader(gzipStream, Charset.defaultCharset());
        BufferedReader reader = new BufferedReader(new InputStreamReader(zipStream));
        return reader.lines().onClose(() -> closeSafely(reader));
    }

    private static void closeSafely(Closeable closeable) {
        if (closeable != null) {
          try {
            closeable.close();
          } catch (IOException e) {
            // Ignore
          }
        }
    }
}
