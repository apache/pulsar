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
package org.apache.pulsar.client.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JavaStringHash implements Hash {
    private static final JavaStringHash instance = new JavaStringHash();

    private JavaStringHash(){ }

    public static Hash getInstance() {
        return instance;
    }

    @Override
    public int makeHash(String s) {
        return s.hashCode() & Integer.MAX_VALUE;
    }

    @Override
    public int makeHash(byte[] b) {
        return makeHash(new String(b, UTF_8));
    }

}
