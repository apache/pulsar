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
 */

/*
 * This class was adapted from NiFi NAR Utils
 * https://github.com/apache/nifi/tree/master/nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-nar-utils
 */

package org.apache.pulsar.common.nar;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import lombok.CustomLog;

/**
 * A utility class containing a few useful static methods to do typical IO
 * operations.
 *
 */
@CustomLog
public class FileUtils {

    public static final long MILLIS_BETWEEN_ATTEMPTS = 50L;

    public static void ensureDirectoryExistAndCanReadAndWrite(final File dir) throws IOException {
        if (dir.exists() && !dir.isDirectory()) {
            throw new IOException(dir.getAbsolutePath() + " is not a directory");
        } else if (!dir.exists()) {
            final boolean made = dir.mkdirs();
            if (!made) {
                throw new IOException(dir.getAbsolutePath() + " could not be created");
            }
        }
        if (!(dir.canRead() && dir.canWrite())) {
            throw new IOException(dir.getAbsolutePath() + " directory does not have read/write privilege");
        }
    }

    public static void ensureDirectoryExistAndCanRead(final File dir) throws IOException {
        if (dir.exists() && !dir.isDirectory()) {
            throw new IOException(dir.getAbsolutePath() + " is not a directory");
        } else if (!dir.exists()) {
            final boolean made = dir.mkdirs();
            if (!made) {
                throw new IOException(dir.getAbsolutePath() + " could not be created");
            }
        }
        if (!dir.canRead()) {
            throw new IOException(dir.getAbsolutePath() + " directory does not have read privilege");
        }
    }

    private static boolean deleteFile(final File file, final int attempts) {
        if (file == null) {
            return false;
        }
        boolean isGone = false;
        try {
            if (file.exists()) {
                final int effectiveAttempts = Math.max(1, attempts);
                for (int i = 0; i < effectiveAttempts && !isGone; i++) {
                    isGone = file.delete() || !file.exists();
                    if (!isGone && (effectiveAttempts - i) > 1) {
                        FileUtils.sleepQuietly(MILLIS_BETWEEN_ATTEMPTS);
                    }
                }
                if (!isGone) {
                    log.warn().attr("file", file.getAbsolutePath())
                            .log("File appears to exist but unable to delete file");
                }
            }
        } catch (final Throwable t) {
            log.warn().attr("file", file.getAbsolutePath()).exception(t).log("Unable to delete file");
        }
        return isGone;
    }

    /**
     * Deletes all files (not directories) in the given directory (recursive)
     * that match the given filename filter. If any file cannot be deleted then
     * this is printed at warn to the given logger.
     *
     * @param directory to delete contents of
     * @param filter if null then no filter is used
     * @param recurse will look for contents of sub directories.
     * @param deleteEmptyDirectories default is false; if true will delete
     * directories found that are empty
     * @throws IOException if abstract pathname does not denote a directory, or
     * if an I/O error occurs
     */
    public static void deleteFilesInDirectory(
        final File directory, final FilenameFilter filter,
        final boolean recurse, final boolean deleteEmptyDirectories) throws IOException {
        // ensure the specified directory is actually a directory and that it exists
        if (null != directory && directory.isDirectory()) {
            final File ingestFiles[] = directory.listFiles();
            if (ingestFiles == null) {
                // null if abstract pathname does not denote a directory, or if an I/O error occurs
                throw new IOException("Unable to list directory content in: " + directory.getAbsolutePath());
            }
            for (File ingestFile : ingestFiles) {
                boolean process = (filter == null) ? true : filter.accept(directory, ingestFile.getName());
                if (ingestFile.isFile() && process) {
                    FileUtils.deleteFile(ingestFile, 3);
                }
                if (ingestFile.isDirectory() && recurse) {
                    FileUtils.deleteFilesInDirectory(ingestFile, filter, recurse, deleteEmptyDirectories);
                    String[] ingestFileList = ingestFile.list();
                    if (deleteEmptyDirectories && ingestFileList != null && ingestFileList.length == 0) {
                        FileUtils.deleteFile(ingestFile, 3);
                    }
                }
            }
        }
    }

    /**
     * Deletes given files.
     *
     * @param files to delete
     * @param recurse will recurse
     * @throws IOException if issues deleting files
     */
    public static void deleteFiles(final Collection<File> files, final boolean recurse) throws IOException {
        for (final File file : files) {
            FileUtils.deleteFile(file, recurse);
        }
    }

    public static void deleteFile(final File file, final boolean recurse) throws IOException {
        final File[] list = file.listFiles();
        if (file.isDirectory() && recurse && list != null) {
            FileUtils.deleteFiles(Arrays.asList(list), recurse);
        }
        //now delete the file itself regardless of whether it is plain file or a directory
        if (!FileUtils.deleteFile(file, 5)) {
            throw new IOException("Unable to delete " + file.getAbsolutePath());
        }
    }

    public static void sleepQuietly(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ex) {
            /* do nothing */
        }
    }

    public static boolean mayBeANarArchive(File jarFile) {
        try (ZipFile zipFile = new ZipFile(jarFile);) {
            ZipEntry entry = zipFile.getEntry("META-INF/bundled-dependencies");
            if (entry == null || !entry.isDirectory()) {
                log.info().attr("jarFile", jarFile)
                        .log("Jar file does not contain META-INF/bundled-dependencies,"
                                + " it is not a NAR file");
                return false;
            } else {
                log.info().attr("jarFile", jarFile)
                        .log("Jar file contains META-INF/bundled-dependencies,"
                                + " it may be a NAR file");
                return true;
            }
        } catch (IOException err) {
            log.info().attr("jarFile", jarFile).exception(err).log("Cannot safely detect if file is a NAR archive");
            return true;
        }
    }
}

