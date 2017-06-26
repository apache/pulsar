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

/**
 * Service used to provide the native memory address of direct byte buffers.
 */
public interface DirectByteBufferAccess {

    /**
     * Returns the native memory address of the given direct byte buffer, or 0
     * if the buffer is not direct or if obtaining the address is not supported.
     *
     * @param buffer the direct byte buffer for which to obtain the address
     * @return the native memory address or 0
     */
    long getAddress(ByteBuffer buffer);
}
