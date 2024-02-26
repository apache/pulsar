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

/**
 * This class was adapted from NiFi NAR Utils
 * https://github.com/apache/nifi/tree/master/nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-nar-utils
 */

package org.apache.pulsar.common.nar;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class to unpack NARs.
 */
@Slf4j
public class NarUnpacker {
    private static final ConcurrentHashMap<String, Object> CURRENT_JVM_FILE_LOCKS = new ConcurrentHashMap<>();

    /**
     * Unpacks the specified nar into the specified base working directory.
     *
     * @param nar
     *            the nar to unpack
     * @param baseWorkingDirectory
     *            the directory to unpack to
     * @return the directory to the unpacked NAR
     * @throws IOException
     *             if unable to explode nar
     */
    public static File unpackNar(final File nar, final File baseWorkingDirectory) throws IOException {
        return doUnpackNar(nar, baseWorkingDirectory, null);
    }

    @VisibleForTesting
    static File doUnpackNar(final File nar, final File baseWorkingDirectory, Runnable extractCallback)
            throws IOException {
        File parentDirectory = new File(baseWorkingDirectory, nar.getName() + "-unpacked");
        if (!parentDirectory.exists()) {
            if (parentDirectory.mkdirs()) {
                log.info("Created directory {}", parentDirectory);
            } else if (!parentDirectory.exists()) {
                throw new IOException("Cannot create " + parentDirectory);
            }
        }
        String md5Sum = Base64.getUrlEncoder().withoutPadding().encodeToString(calculateMd5sum(nar));
        // ensure that one process can extract the files
        File lockFile = new File(parentDirectory, "." + md5Sum + ".lock");
        // prevent OverlappingFileLockException by ensuring that one thread tries to create a lock in this JVM
        Object localLock = CURRENT_JVM_FILE_LOCKS.computeIfAbsent(lockFile.getAbsolutePath(), key -> new Object());
        synchronized (localLock) {
            // create file lock that ensures that other processes
            // using the same lock file don't execute concurrently
            try (FileChannel channel = new RandomAccessFile(lockFile, "rw").getChannel();
                 FileLock lock = channel.lock()) {
                File narWorkingDirectory = new File(parentDirectory, md5Sum);
                if (narWorkingDirectory.mkdir()) {
                    try {
                        log.info("Extracting {} to {}", nar, narWorkingDirectory);
                        if (extractCallback != null) {
                            extractCallback.run();
                        }
                        unpack(nar, narWorkingDirectory);
                    } catch (IOException e) {
                        log.error("There was a problem extracting the nar file. Deleting {} to clean up state.",
                                narWorkingDirectory, e);
                        FileUtils.deleteFile(narWorkingDirectory, true);
                        throw e;
                    }
                }
                return narWorkingDirectory;
            }
        }
    }

    /**
     * Unpacks the NAR to the specified directory.
     *
     * @param workingDirectory
     *            the root directory to which the NAR should be unpacked.
     * @throws IOException
     *             if the NAR could not be unpacked.
     */
    private static void unpack(final File nar, final File workingDirectory) throws IOException {
        Path workingDirectoryPath = workingDirectory.toPath().normalize();
        try (ZipFile zipFile = new ZipFile(nar)) {
            Enumeration<? extends ZipEntry> zipEntries = zipFile.entries();
            while (zipEntries.hasMoreElements()) {
                ZipEntry zipEntry = zipEntries.nextElement();
                String name = zipEntry.getName();
                Path targetFilePath = workingDirectoryPath.resolve(name).normalize();
                if (!targetFilePath.startsWith(workingDirectoryPath)) {
                    log.error("Invalid zip file with entry '{}'", name);
                    throw new IOException("Invalid zip file. Aborting unpacking.");
                }
                File f = targetFilePath.toFile();
                if (zipEntry.isDirectory()) {
                    FileUtils.ensureDirectoryExistAndCanReadAndWrite(f);
                } else {
                    makeFile(zipFile.getInputStream(zipEntry), f);
                }
            }
        }
    }

    /**
     * Creates the specified file, whose contents will come from the <tt>InputStream</tt>.
     *
     * @param inputStream
     *            the contents of the file to create.
     * @param file
     *            the file to create.
     * @throws IOException
     *             if the file could not be created.
     */
    private static void makeFile(final InputStream inputStream, final File file) throws IOException {
        try (final InputStream in = inputStream; final FileOutputStream fos = new FileOutputStream(file)) {
            byte[] bytes = new byte[65536];
            int numRead;
            while ((numRead = in.read(bytes)) != -1) {
                fos.write(bytes, 0, numRead);
            }
        }
    }

    /**
     * Calculates an md5 sum of the specified file.
     *
     * @param file
     *            to calculate the md5sum of
     * @return the md5sum bytes
     * @throws IOException
     *             if cannot read file
     */
    private static byte[] calculateMd5sum(final File file) throws IOException {
        try (final FileInputStream inputStream = new FileInputStream(file)) {
            final MessageDigest md5 = MessageDigest.getInstance("md5");

            final byte[] buffer = new byte[1024];
            int read = inputStream.read(buffer);

            while (read > -1) {
                md5.update(buffer, 0, read);
                read = inputStream.read(buffer);
            }

            return md5.digest();
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalArgumentException(nsae);
        }
    }
}
