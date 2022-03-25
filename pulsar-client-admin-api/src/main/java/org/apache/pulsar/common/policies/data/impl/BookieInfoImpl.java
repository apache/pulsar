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
package org.apache.pulsar.common.policies.data.impl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.pulsar.common.policies.data.BookieInfo;

/**
 * Bookie information.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class BookieInfoImpl implements BookieInfo {
    private String rack;
    private String hostname;

    public static BookieInfoImplBuilder builder() {
        return new BookieInfoImplBuilder();
    }

    public static class BookieInfoImplBuilder implements BookieInfo.Builder {
        private String rack;
        private String hostname;
        private static final String PATH_SEPARATOR = "/";

        public BookieInfoImplBuilder rack(String rack) {
            this.rack = rack;
            return this;
        }

        public BookieInfoImplBuilder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public BookieInfoImpl build() {
            checkArgument(rack != null && !rack.isEmpty() && !rack.equals(PATH_SEPARATOR),
                    "rack name is invalid, it should not be null, empty or '/'");
            return new BookieInfoImpl(rack, hostname);
        }

        public static void checkArgument(boolean expression, @NonNull Object errorMessage) {
            if (!expression) {
                throw new IllegalArgumentException(String.valueOf(errorMessage));
            }
        }
    }
}
