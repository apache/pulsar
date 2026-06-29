package org.apache.pulsar.common.util;

import io.netty.util.concurrent.FastThreadLocal;

public class StringUtil {

    // Optimized: Replaced standard ThreadLocal with Netty's FastThreadLocal for better performance in Netty event loops
    private static final FastThreadLocal<StringBuilder> threadLocalBuilder = new FastThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
            return new StringBuilder(1024);
        }
    };

    public static StringBuilder getThreadLocalBuilder() {
        StringBuilder builder = threadLocalBuilder.get();
        builder.setLength(0); // Reset for reuse
        return builder;
    }
}
