/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.scurrilous.circe.impl;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Provides access to a singleton {@link DirectByteBufferAccess} implementation,
 * if one is available.
 */
public final class DirectByteBufferAccessLoader {

    private static final DirectByteBufferAccess INSTANCE;

    static {
        final Iterator<DirectByteBufferAccess> iterator = ServiceLoader.load(
                DirectByteBufferAccess.class).iterator();
        INSTANCE = iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Returns the native memory address of the given direct byte buffer, or 0
     * if the buffer is not direct or if obtaining the address is not supported.
     * 
     * @param buffer the direct byte buffer for which to obtain the address
     * @return the native memory address or 0
     */
    public static long getAddress(ByteBuffer buffer) {
        return INSTANCE != null ? INSTANCE.getAddress(buffer) : 0;
    }

    private DirectByteBufferAccessLoader() {
    }
}
