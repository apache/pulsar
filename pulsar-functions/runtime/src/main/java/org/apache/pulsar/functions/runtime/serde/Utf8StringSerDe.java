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
package org.apache.pulsar.functions.runtime.serde;

import static com.google.common.base.Charsets.UTF_8;

/**
 * A simple Serde that treats the bytes as Java String
 */
public class Utf8StringSerDe implements SerDe {

    public static Utf8StringSerDe of() {
        return INSTANCE;
    }

    private static final Utf8StringSerDe INSTANCE = new Utf8StringSerDe();

    @Override
    public byte[] serialize(Object resultValue) {
        String string = (String) resultValue;
        return string.getBytes(UTF_8);
    }

    @Override
    public Object deserialize(byte[] data) {
        return new String(data, UTF_8);
    }
}