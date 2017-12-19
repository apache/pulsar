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
package org.apache.pulsar.functions.runtime.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * A simple Serde that treats the bytes as Java String
 */
public class SimpleStringSerDe implements SerDe {
    private static final Logger log = LoggerFactory.getLogger(SimpleStringSerDe.class);

    public SimpleStringSerDe() { }

    @Override
    public byte[] serialize(Object resultValue) {
        String string = (String) resultValue;
        return string.getBytes();
    }

    @Override
    public Object deserialize(byte[] data) {
        return new String(data);
    }
}