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
package org.apache.bookkeeper.mledger.offload.filesystem;

import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

/**
 *
 * The Index block abstraction used for offload a ledger to long term storage.
 *
 */
@Unstable
public interface OffloadIndexFile extends OffloadIndex {

    /**
     * Writes ledger index data to an index file
     *
     */
    void writeIndexDataIntoFile(FSDataOutputStream indexFileOutputStream) throws IOException;

    /**
     * Init an ledger index file
     */
    OffloadIndexFile initIndexFile(FSDataInputStream indexInputStream) throws IOException;

}

