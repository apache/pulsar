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

/**
 * Enumeration of MurmurHash3 variants.
 */
public enum MurmurHash3Variant {

    /**
     * 32-bit variant (optimized for x86).
     */
    X86_32("MurmurHash3_x86_32"),

    /**
     * 128-bit variant optimized for x86.
     */
    X86_128("MurmurHash3_x86_128"),

    /**
     * 128-bit variant optimized for x64.
     */
    X64_128("MurmurHash3_x64_128");

    private final String algorithm;

    private MurmurHash3Variant(String algorithm) {
        this.algorithm = algorithm;
    }

    /**
     * Returns the algorithm name corresponding to this variant.
     * 
     * @return this variant's algorithm name
     */
    public String algorithm() {
        return algorithm;
    }
}
