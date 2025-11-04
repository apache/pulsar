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
            throw new IllegalArgumentException(String.format("Invalid size '%s'. Valid formats are: %s",
                    byteStr, "(4096, 100K, 10M, 16G, 2T)"));
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
}
