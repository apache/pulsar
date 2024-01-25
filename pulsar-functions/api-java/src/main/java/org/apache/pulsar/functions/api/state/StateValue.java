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
package org.apache.pulsar.functions.api.state;

import java.nio.ByteBuffer;

public class StateValue {
    private final ByteBuffer value;
    private final Long version;
    private final Boolean isNumber;

    public StateValue(ByteBuffer value, Long version, Boolean isNumber) {
        this.value = value == null ? null : ByteBuffer.wrap(value.array());
        this.version = version;
        this.isNumber = isNumber;
    }

    public ByteBuffer getValue() {
        return value == null ? null : ByteBuffer.wrap(value.array());
    }

    public Long getVersion() {
        return version;
    }

    public Boolean getIsNumber() {
        return isNumber;
    }
}