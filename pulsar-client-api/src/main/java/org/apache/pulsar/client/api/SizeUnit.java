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
package org.apache.pulsar.client.api;

/**
 * Size unit converter.
 */
public enum SizeUnit {
    BYTES(1L), KILO_BYTES(1024L), MEGA_BYTES(1024L * 1024L), GIGA_BYTES(1024L * 1024L * 1024L);

    private final long bytes;

    private SizeUnit(long bytes) {
        this.bytes = bytes;
    }

    public long toBytes(long value) {
        return value * bytes;
    }

    public long toKiloBytes(long value) {
        return toBytes(value) / KILO_BYTES.bytes;
    }

    public long toMegaBytes(long value) {
        return toBytes(value) / MEGA_BYTES.bytes;
    }

    public long toGigaBytes(long value) {
        return toBytes(value) / GIGA_BYTES.bytes;
    }
}
