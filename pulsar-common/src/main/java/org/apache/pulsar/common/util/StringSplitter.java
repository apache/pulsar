package org.apache.pulsar.common.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StringSplitter {

    /**
     * Split the string by the given delimiter, it drops the latest delimiter and empty piece.
     */
    public static List<String> splitByChar(String str, char delimiter, int limit) {
        if (str == null) {
            return Collections.emptyList();
        }
        List<String> parts = new ArrayList<>(Math.max(limit, 0));
        int length = str.length();
        int start = 0;
        int count = 0;
        int effectiveLimit = (limit <= 0) ? Integer.MAX_VALUE : limit - 1;
        for (int i = 0; i < length && count < effectiveLimit; i++) {
            if (str.charAt(i) == delimiter) {
                // Drop the empty piece.
                if (start == i) {
                    start = i + 1;
                    continue;
                }
                parts.add(str.substring(start, i));
                start = i + 1;
                count++;
            }
        }
        if (start < length) {
            parts.add(str.substring(start));
        }
        return parts;
    }
}
