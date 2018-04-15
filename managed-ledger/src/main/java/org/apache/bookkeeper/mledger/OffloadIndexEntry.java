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
package org.apache.bookkeeper.mledger;

import com.google.common.annotations.Beta;

/**
 *
 * The Index Entry in OffloadIndexBlock.
 * It consists of the message entry id, the code storage block part id for this message entry,
 * and the offset in code storage block for this message id.
 *
 */
@Beta
public interface OffloadIndexEntry {

    /**
     * Get the entryId that this entry contains.
     */
    long getEntryId();

    /**
     * Get the block part id of code storage.
     */
    int getPartId();

    /**
     * Get the offset of this message entry in code storage.
     */
    long getOffset();
}

