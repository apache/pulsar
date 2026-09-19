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
package org.apache.pulsar.client.impl.v5;

import java.security.SecureRandom;

/**
 * Random short identifiers used for default consumer / producer names that the V5
 * builders generate when the user did not provide one.
 *
 * <p>Eight alphanumeric characters from a {@link SecureRandom} source give ~48 bits of
 * entropy — enough to avoid collisions in practice while keeping log lines and broker
 * registration keys short.
 */
final class V5RandomIds {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final String ALPHABET =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private V5RandomIds() {
    }

    /**
     * Returns an alphanumeric random string of the given length.
     */
    static String randomAlphanumeric(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
        }
        return sb.toString();
    }
}
