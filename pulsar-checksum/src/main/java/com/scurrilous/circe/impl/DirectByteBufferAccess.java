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
