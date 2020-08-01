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
/*
 * The original MurmurHash3 was written by Austin Appleby, and is placed in the
 * public domain. This source code, implemented by Licht Takeuchi, is based on
 * the orignal MurmurHash3 source code.
 */
package org.apache.pulsar.client.impl;

import java.nio.charset.StandardCharsets;

public class Murmur3_32Hash implements Hash {
    private static final Murmur3_32Hash instance = new Murmur3_32Hash();

    private Murmur3_32Hash(){ }

    public static Hash getInstance() {
        return instance;
    }

    @Override
    public int makeHash(String s) {
        return org.apache.pulsar.common.util.Murmur3_32Hash.getInstance()
            .makeHash(s.getBytes(StandardCharsets.UTF_8)) & Integer.MAX_VALUE;
    }

    @Override
    public int makeHash(byte[] b) {
        return org.apache.pulsar.common.util.Murmur3_32Hash.getInstance().makeHash(b) & Integer.MAX_VALUE;
    }
}
