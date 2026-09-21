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

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

/**
 * Deduplicates String instances by interning them using Guava's Interner
 * which is more efficient than String.intern().
 */
public class StringInterner {
    private static final StringInterner INSTANCE = new StringInterner();
    private final Interner<String> interner;

    public static String intern(String sample) {
        return INSTANCE.doIntern(sample);
    }

    private StringInterner() {
        this.interner = Interners.newWeakInterner();
    }

    String doIntern(String sample) {
        if (sample == null) {
            return null;
        }
        return interner.intern(sample);
    }
}
