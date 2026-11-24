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
package org.apache.pulsar.cli;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class ValueValidationUtil {

    public static void maxValueCheck(String paramName, long value, long maxValue) {
        if (value > maxValue) {
            throw new IllegalArgumentException(paramName + " cannot be bigger than <" + maxValue + ">!");
        }
    }

    public static void positiveCheck(String paramName, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException(paramName + " cannot be less than or equal to <0>!");
        }
    }

    public static void positiveCheck(String paramName, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(paramName + " cannot be less than or equal to <0>!");
        }
    }

    public static void emptyCheck(String paramName, String value) {
        if (StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("The value of " + paramName + " can't be empty");
        }
    }

    public static void minValueCheck(String name, Long value, long min) {
        if (value < min) {
            throw new IllegalArgumentException(name + " cannot be less than <" + min + ">!");
        }
    }
}
