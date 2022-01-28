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
package org.apache.pulsar.packages.management.core.common;

/**
 * PackageType used to identify the package type. Currently we have three types of packages, function, sink and source.
 */
public enum PackageType {
    FUNCTION("function"), SINK("sink"), SOURCE("source");

    private final String value;

    PackageType(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    public static PackageType getEnum(String value) {
        for (PackageType e : values()) {
            if (e.value.equalsIgnoreCase(value)) {
                return e;
            }
        }
        throw new IllegalArgumentException("Invalid package domain: '" + value + "'");
    }

    @Override
    public String toString() {
        return this.value;
    }
}
