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
package org.apache.pulsar.io.core.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Annotation for documenting fields in a config.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface FieldDoc {

    /**
     * Return if the field is required or not.
     *
     * @return true if the field is required, otherwise false
     */
    boolean required() default false;

    /**
     * Return the value of this field.
     *
     * @return the default value of this field
     */
    String defaultValue();

    /**
     * Return if the field is a sensitive type or not.
     * User name, password, access token are some examples of sensitive fields.
     *
     * @return true if the field is sensitive, otherwise false
     */
    boolean sensitive() default false;

    /**
     * Return the description of this field.
     *
     * @return the help message of this field
     */
    String help();

}
