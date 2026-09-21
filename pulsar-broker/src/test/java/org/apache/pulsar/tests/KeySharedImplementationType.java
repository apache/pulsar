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
package org.apache.pulsar.tests;

import java.util.Arrays;
import java.util.function.Function;
import org.testng.SkipException;

/**
 * KeyShared implementation type used in test.
 */
public enum KeySharedImplementationType {
    // default implementation, PIP-379
    PIP379(false),
    // classic implementation before PIP-282 and PIP-379
    Classic(true);

    public static final KeySharedImplementationType DEFAULT = PIP379;
    public final boolean classic;

    KeySharedImplementationType(boolean classic) {
        this.classic = classic;
    }

    public void skipIfClassic() {
        if (classic) {
            throw new SkipException("Test is not applicable for classic implementation");
        }
    }

    public Object[][] prependImplementationTypeToData(Object[][] data) {
        return Arrays.stream(data)
                .map(array -> {
                    Object[] newArray = new Object[array.length + 1];
                    newArray[0] = this;
                    System.arraycopy(array, 0, newArray, 1, array.length);
                    return newArray;
                })
                .toArray(Object[][]::new);
    }

    public static Object[] generateTestInstances(Function<KeySharedImplementationType, Object> testInstanceFactory) {
        return Arrays.stream(KeySharedImplementationType.values()).map(testInstanceFactory).toArray();
    }
}
