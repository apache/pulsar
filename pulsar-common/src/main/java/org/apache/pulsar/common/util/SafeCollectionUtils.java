/**
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Safe collection utils.
 */
public class SafeCollectionUtils {

    public static List<Long> longArrayToList(long[] array) {
        if (array == null || array.length == 0) {
            return Collections.emptyList();
        } else {
            List<Long> result = new ArrayList<>(array.length);
            for (long l : array) {
                result.add(l);
            }
            return result;
        }
    }

    public static long[] longListToArray(List<Long> list) {
        if (list == null || list.size() == 0) {
            return new long[0];
        } else {
            long[] array = new long[list.size()];
            for (int i = 0; i < list.size(); i++) {
                array[i] = list.get(i);
            }
            return array;
        }
    }
}
