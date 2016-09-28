/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.scurrilous.circe.params;

import com.scurrilous.circe.HashParameters;

/**
 * Hash parameters consisting only of an algorithm name. Includes instances
 * describing some commonly used algorithms.
 */
public class SimpleHashParameters implements HashParameters {

    private final String algorithm;

    /**
     * Constructs a {@link SimpleHashParameters} with the given algorithm name.
     * 
     * @param algorithm the name of the hash algorithm
     */
    public SimpleHashParameters(String algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public String algorithm() {
        return algorithm;
    }

    @Override
    public int hashCode() {
        return algorithm.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != SimpleHashParameters.class)
            return false;
        return algorithm.equals(((SimpleHashParameters) obj).algorithm);
    }

    @Override
    public String toString() {
        return algorithm;
    }

    /**
     * Represents the MD5 (128-bit) hash algorithm.
     */
    public static final SimpleHashParameters MD5 = new SimpleHashParameters("MD5");

    /**
     * Represents the SHA-1 (160-bit) hash algorithm.
     */
    public static final SimpleHashParameters SHA1 = new SimpleHashParameters("SHA-1");

    /**
     * Represents the SHA-256 (256-bit) hash algorithm.
     */
    public static final SimpleHashParameters SHA256 = new SimpleHashParameters("SHA-256");

    /**
     * Represents the SHA-384 (384-bit) hash algorithm.
     */
    public static final SimpleHashParameters SHA384 = new SimpleHashParameters("SHA-384");

    /**
     * Represents the SHA-512 (512-bit) hash algorithm.
     */
    public static final SimpleHashParameters SHA512 = new SimpleHashParameters("SHA-512");
}
