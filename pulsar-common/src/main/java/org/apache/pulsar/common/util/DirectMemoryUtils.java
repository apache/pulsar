package org.apache.pulsar.common.util;

// CHECKSTYLE.OFF: IllegalImport
import io.netty.util.internal.PlatformDependent;
// CHECKSTYLE.ON: IllegalImport

public class DirectMemoryUtils {

    /**
     * PlatformDependent.maxDirectMemory can be inaccurate if java property `io.netty.maxDirectMemory` are setted.
     * Cache the result in this field.
     */
    public static final long JVM_MAX_DIRECT_MEMORY = PlatformDependent.estimateMaxDirectMemory();

}
