/*
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
package org.apache.pulsar.cli.converters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ByteUnitUtil {

    private static final String VALID_FORMATS = "(4096, 100K, 10M, 16G, 2T)";
    private static final long KIB = 1024L;
    private static final long MIB = KIB * 1024;
    private static final long GIB = MIB * 1024;
    private static final long TIB = GIB * 1024;
    private static Set<Character> sizeUnit = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList('k', 'K', 'm', 'M', 'g', 'G', 't', 'T')));

    public static long validateSizeString(String byteStr) {
        if (byteStr.isEmpty()) {
            throw new IllegalArgumentException("byte string cannot be empty");
        }

        char last = byteStr.charAt(byteStr.length() - 1);
        String subStr = byteStr.substring(0, byteStr.length() - 1);
        long size;
        try {
            size = sizeUnit.contains(last)
                    ? Long.parseLong(subStr)
                    : Long.parseLong(byteStr);
        } catch (IllegalArgumentException e) {
            throw invalidSize(byteStr, e);
        }
        switch (last) {
            case 'k':
            case 'K':
                return multiplyExact(byteStr, size, KIB);

            case 'm':
            case 'M':
                return multiplyExact(byteStr, size, MIB);

            case 'g':
            case 'G':
                return multiplyExact(byteStr, size, GIB);

            case 't':
            case 'T':
                return multiplyExact(byteStr, size, TIB);

            default:
                return size;
        }
    }

    private static long multiplyExact(String byteStr, long size, long multiplier) {
        try {
            return Math.multiplyExact(size, multiplier);
        } catch (ArithmeticException e) {
            throw invalidSize(byteStr, e);
        }
    }

    private static IllegalArgumentException invalidSize(String byteStr, Exception cause) {
        return new IllegalArgumentException(String.format("Invalid size '%s'. Valid formats are: %s",
                byteStr, VALID_FORMATS), cause);
    }
}
