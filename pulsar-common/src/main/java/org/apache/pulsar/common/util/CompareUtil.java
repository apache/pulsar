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
package org.apache.pulsar.common.util;

import lombok.experimental.UtilityClass;

/**
 * Utility class for comparing values.
 */
@UtilityClass
public class CompareUtil {

    /**
     * Compare two double values with a given resolution.
     * @param v1 first value to compare
     * @param v2 second value to compare
     * @param resolution resolution to compare with
     * @return -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
     */
    public static int compareDoubleWithResolution(double v1, double v2, double resolution) {
        return Long.compare((long) (v1 / resolution), (long) (v2 / resolution));
    }

    /**
     * Compare two long values with a given resolution.
     * @param v1 first value to compare
     * @param v2 second value to compare
     * @param resolution resolution to compare with
     * @return -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
     */
    public static int compareLongWithResolution(long v1, long v2, long resolution) {
        return Long.compare(v1 / resolution, v2 / resolution);
    }

    /**
     * Compare two int values with a given resolution.
     * @param v1 first value to compare
     * @param v2 second value to compare
     * @param resolution resolution to compare with
     * @return -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
     */
    public static int compareIntegerWithResolution(int v1, int v2, int resolution) {
        return Integer.compare(v1 / resolution, v2 / resolution);
    }
}
