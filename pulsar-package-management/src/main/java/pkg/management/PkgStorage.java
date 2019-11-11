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

package pkg.management;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

// Package storage is used to store the package files.
public interface PkgStorage {

    // Write file using InputStream to a path
    CompletableFuture<Void> write(String path, InputStream inputStream);

    // Write file using bytes to a path
    CompletableFuture<Void> write(String path, byte[] data);

    // Read file using OutputStream from a path
    CompletableFuture<Void> read(String path, OutputStream outputStream);

    // Read file using bytes to a path
    CompletableFuture<byte[]> read(String path);

    // Delete a file at path
    CompletableFuture<Void> delete(String path);

    // List all the file under a path
    CompletableFuture<List<String>> list(String path);

    // Check the file at a path is or not exists
    CompletableFuture<Boolean> exist(String path);
}
