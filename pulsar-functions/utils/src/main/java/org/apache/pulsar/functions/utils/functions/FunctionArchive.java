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
package org.apache.pulsar.functions.utils.functions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.functions.utils.FunctionFilePackage;
import org.apache.pulsar.functions.utils.ValidatableFunctionPackage;

public class FunctionArchive implements AutoCloseable {
    private final Path archivePath;
    /** SHA-256 hex of archive file contents; empty when {@link #archivePath} is null (test doubles). */
    private final String archiveChecksumHex;
    private final FunctionDefinition functionDefinition;
    private final String narExtractionDirectory;
    private final boolean enableClassloading;
    private ValidatableFunctionPackage functionPackage;
    private boolean closed;

    public FunctionArchive(Path archivePath, FunctionDefinition functionDefinition, String narExtractionDirectory,
                           boolean enableClassloading) {
        this(archivePath, functionDefinition, narExtractionDirectory, enableClassloading, null);
    }

    /**
     * @param precomputedArchiveChecksumHex SHA-256 hex of {@code archivePath} contents; if null and path is non-null,
     *                                   the hash is computed once at construction time.
     */
    public FunctionArchive(Path archivePath, FunctionDefinition functionDefinition, String narExtractionDirectory,
                           boolean enableClassloading, String precomputedArchiveChecksumHex) {
        this.archivePath = archivePath;
        this.functionDefinition = functionDefinition;
        this.narExtractionDirectory = narExtractionDirectory;
        this.enableClassloading = enableClassloading;
        if (archivePath != null) {
            try {
                this.archiveChecksumHex = precomputedArchiveChecksumHex != null
                        ? precomputedArchiveChecksumHex
                        : FunctionUtils.computeArchiveChecksumHex(archivePath);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            this.archiveChecksumHex = "";
        }
    }

    public Path getArchivePath() {
        return archivePath;
    }

    public String getArchiveChecksumHex() {
        return archiveChecksumHex;
    }

    public synchronized ValidatableFunctionPackage getFunctionPackage() {
        if (closed) {
            throw new IllegalStateException("FunctionArchive is already closed");
        }
        if (functionPackage == null) {
            functionPackage = new FunctionFilePackage(archivePath.toFile(), narExtractionDirectory, enableClassloading,
                    FunctionDefinition.class);
        }
        return functionPackage;
    }

    public FunctionDefinition getFunctionDefinition() {
        return functionDefinition;
    }

    @Override
    public synchronized void close() throws Exception {
        closed = true;
        if (functionPackage instanceof AutoCloseable) {
            ((AutoCloseable) functionPackage).close();
        }
    }
}
