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
package org.apache.pulsar.common.policies.data.impl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.pulsar.common.policies.data.ExtBookieInfo;

/**
 * The ExtBookieInfoImpl class is an extension of the BookieInfoImpl class,
 * containing additional detailed information.
 * <p>
 * In addition to the rack and hostname attributes, it also includes
 * address and group attributes.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class ExtBookieInfoImpl implements ExtBookieInfo {
    private String address;
    private String group;
    private String rack;
    private String hostname;

    public static ExtBookieInfoImplBuilder builder() {
        return new ExtBookieInfoImplBuilder();
    }

    public static class ExtBookieInfoImplBuilder implements ExtBookieInfo.Builder {
        private String address;
        private String group;
        private String rack;
        private String hostname;
        private static final String PATH_SEPARATOR = "/";

        public ExtBookieInfoImplBuilder address(String address) {
            this.address = address;
            return this;
        }

        public ExtBookieInfoImplBuilder group(String group) {
            this.group = group;
            return this;
        }

        public ExtBookieInfoImplBuilder rack(String rack) {
            this.rack = rack;
            return this;
        }

        public ExtBookieInfoImplBuilder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public ExtBookieInfoImpl build() {
            checkArgument(rack != null && !rack.isEmpty() && !rack.equals(PATH_SEPARATOR),
                    "rack name is invalid, it should not be null, empty or '/'");
            return new ExtBookieInfoImpl(address, group, rack, hostname);
        }

        public static void checkArgument(boolean expression, @NonNull Object errorMessage) {
            if (!expression) {
                throw new IllegalArgumentException(String.valueOf(errorMessage));
            }
        }
    }
}
