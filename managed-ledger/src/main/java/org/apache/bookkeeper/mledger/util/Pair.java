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
package org.apache.bookkeeper.mledger.util;

import com.google.common.base.Objects;

/**
 * A generic container for two values.
 *
 * @param <FirstT>
 *     the first value type
 * @param <SecondT>
 *     the second value type
 */
public class Pair<FirstT, SecondT> {
    public final FirstT first;
    public final SecondT second;

    public static <FirstT, SecondT> Pair<FirstT, SecondT> create(FirstT x, SecondT y) {
        return new Pair<>(x, y);
    }

    public Pair(FirstT first, SecondT second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return String.format("(%s,%s)", first, second);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object obj) {
        if (obj instanceof Pair) {
            Pair other = (Pair) obj;
            return Objects.equal(first, other.first) && Objects.equal(second, other.second);
        }

        return false;
    }
}
