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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;

/**
 * Helper class that provides helper methods for working with
 * gzip-formatted files.
 */
public class GZipFiles {

    /**
     * Returns true if the given file is a gzip file.
     */
    public static boolean isGzip(File f) {

       InputStream input = null;
        try {
            input = new FileInputStream(f);
            PushbackInputStream pb = new PushbackInputStream(input, 2);
            byte [] signature = new byte[2];
            int len = pb.read(signature); //read the signature
            pb.unread(signature, 0, len); //push back the signature to the stream
            // check if matches standard gzip magic number
            return (signature[ 0 ] == (byte) 0x1f && signature[1] == (byte) 0x8b);
        } catch (final Exception e) {
            return false;
        } finally {
            IOUtils.closeQuietly(input);
        }
    }

    /**
     * Get a lazily loaded stream of lines from a gzipped file, similar to
     * {@link Files#lines(java.nio.file.Path)}.
     *
     * @param path
     *          The path to the gzipped file.
     * @return stream with lines.
     */
    public static Stream<String> lines(Path path) {
      GZIPInputStream gzipStream = null;

      try {
        gzipStream = new GZIPInputStream(Files.newInputStream(path));
      } catch (IOException e) {
        closeSafely(gzipStream);
        throw new UncheckedIOException(e);
      }

      BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream));
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
