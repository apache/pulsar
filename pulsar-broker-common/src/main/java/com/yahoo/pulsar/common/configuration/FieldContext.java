/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.common.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Stores field context to validate based on requirement or value constraints.
 * 
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface FieldContext {

    /**
     * checks field value is required. By default field is mandatory false.
     * 
     * @return true if attribute is required else returns false
     */
    public boolean required() default false;

    /**
     * binds numeric value's lower bound
     * 
     * @return minimum value of the field
     */
    public long minValue() default Long.MIN_VALUE;

    /**
     * binds numeric value's upper bound
     * 
     * @return maximum value of the field
     */
    public long maxValue() default Long.MAX_VALUE;

    /**
     * binds character length of text
     * 
     * @return character length of field
     */
    public int maxCharLength() default Integer.MAX_VALUE;
    
    /**
     * allow field to be updated dynamically
     * 
     * @return
     */
    public boolean dynamic() default false;
}
