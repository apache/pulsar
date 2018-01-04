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
package org.apache.pulsar.functions.annotation;

import java.lang.reflect.Field;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * A utility class to verify annotations.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Annotations {

    /**
     * Verify if all the required fields in <tt>object</tt> are provided.
     *
     * @param object object to verify
     * @return true if all the required fields are set, otherwise false.
     */
    public static boolean verifyAllRequiredFieldsSet(Object object) {
        try {
            for (Field f : object.getClass().getDeclaredFields()) {
                // ignore non required field
                if (!f.isAnnotationPresent(RequiredField.class)) {
                    continue;
                }

                f.setAccessible(true);

                if (f.get(object) == null) {
                    return false;
                }
            }
        } catch (IllegalAccessException ex) {
            return false;
        }
        return true;
    }


}
