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

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.client.api.ReadHandle;

import java.io.IOException;
import java.util.Map;

/**
 * The BlockAwareSegmentInputStream for each cold storage data block.
 * This interface should be implemented while extends InputStream.
 * It gets data from ledger, and will be read out the content for a data block.
 * DataBlockHeader + entries(each with format[[entry_size -- int][entry_id -- long][entry_data]]) + padding
 */
public abstract class FileSystemEntryBytesReader {

    protected final ReadHandle readHandle;

    protected static final int DATA_FILE_MAGIC_WORD = 0xFBDBABCB;

    // This is bigger than header size. Leaving some place for alignment and future enhancement.
    // Payload use this as the start offset.
    protected static final int HEADER_SIZE = 128;

    protected static final int HEADER_UN_USE_SIZE = HEADER_SIZE - 4;

    // buf the entry size and entry id.
    protected static final int ENTRY_HEADER_SIZE = 4 /* entry size */ + 8 /* entry id */;

    // how many entries want to read from ReadHandle each time.
    protected static final int ENTRIES_PER_READ = 100;

    protected static final int ADD_INDEX_PER_WRITTEN_COUNT = 100;

    protected static final int ADD_INDEX_PER_WRITTEN_BYTES_SIZE = 1024*1024;

    protected long haveOffloadEntryCount;

    // have written byte size into file
    protected int haveWrittenBytes = HEADER_SIZE;


    protected boolean canContinueRead = true;

        public abstract ByteBuf readEntries() throws IOException;

    protected FileSystemEntryBytesReader(ReadHandle readHandle, Map<String, String> configMap) {
        this.readHandle = readHandle;
    }

    public int getHeaderSize() {
        return HEADER_SIZE;
    }

    public int getDataFileMagicWord() {
        return DATA_FILE_MAGIC_WORD;
    }

    public int getHeaderUnUseSize() {
        return HEADER_UN_USE_SIZE;
    }

    public long getDataObjectLength() {
        return haveWrittenBytes - HEADER_SIZE;
    }

    public static int getDataHeaderLength() {
        return HEADER_SIZE;
    }


    /**
     * Get entry count that read out from this InputStream
     *
     * @return the block entry count
     */
    public abstract int getEntryCount();

    /**
     * Get end entry id contained in this InputStream.
     *
     * @return the end entry id
     */
    public abstract long getEndEntryId();

    /**
     * Get sum of entries data size read from the this InputStream
     *
     * @return the block entry bytes count
     */
    public abstract int getEntryBytesCount();

    public boolean whetherCanContinueRead() {
        return canContinueRead;
    }
}
