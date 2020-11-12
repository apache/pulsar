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
package org.apache.pulsar.config.validation;

import java.util.Map;

/**
 * Primary validation interface. All validators should extend this abstract class and implement
 * the validateField method.
 */
public abstract class Validator {
    public Validator(Map<String, Object> params) {
    }

    public Validator() {
    }

    /**
     * validate the field value o that belogs to the field which is named name
     * This method should throw IllegalArgumentException in case o doesn't
     * validate per this validator's implementation.
     */
    public abstract void validateField(String name, Object o);
}