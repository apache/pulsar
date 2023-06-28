package org.apache.pulsar.testclient;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 * @see org.apache.pulsar.client.cli.MemoryUnitToByteConverter
 */
class MemoryUnitToByteConverter implements IStringConverter<Long> {

    private static Set<Character> sizeUnit = Sets.newHashSet('k', 'K', 'm', 'M', 'g', 'G', 't', 'T');
    private static final long DEFAULT_MEMORY_LIMIT = 0L;

    @Override
    public Long convert(String memoryLimitArgument) {
        return Math.max(DEFAULT_MEMORY_LIMIT, parseBytes(memoryLimitArgument));
    }

    public static long parseBytes(String memoryLimitArgument) {
        if (StringUtils.isNotEmpty(memoryLimitArgument)) {
            long memoryLimitArg = validateSizeString(memoryLimitArgument);
            if (positiveCheckStatic("memory-limit", memoryLimitArg)) {
                return memoryLimitArg;
            }
        }
        return DEFAULT_MEMORY_LIMIT;
    }

    static long validateSizeString(String s) {
        char last = s.charAt(s.length() - 1);
        String subStr = s.substring(0, s.length() - 1);
        long size;
        try {
            size = sizeUnit.contains(last)
                    ? Long.parseLong(subStr)
                    : Long.parseLong(s);
        } catch (IllegalArgumentException e) {
            throw new ParameterException(String.format("Invalid size '%s'. Valid formats are: %s",
                    s, "(4096, 100K, 10M, 16G, 2T)"));
        }
        switch (last) {
            case 'k':
            case 'K':
                return size * 1024;

            case 'm':
            case 'M':
                return size * 1024 * 1024;

            case 'g':
            case 'G':
                return size * 1024 * 1024 * 1024;

            case 't':
            case 'T':
                return size * 1024 * 1024 * 1024 * 1024;

            default:
                return size;
        }
    }

    static boolean positiveCheckStatic(String paramName, long value) {
        if (value <= 0) {
            throw new ParameterException(paramName + " is not be negative or 0!");
        }
        return true;
    }
}
