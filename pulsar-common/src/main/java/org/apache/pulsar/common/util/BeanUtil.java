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
package org.apache.pulsar.common.util;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utils related to bean.
 */
public class BeanUtil {

    /**
     * Copy property values from the source bean to the target bean for all cases where the property names and types are
     * the same.
     * This is just a convenience method. For more complex transfer needs, consider using other BeanUtils
     *
     * @param source          – the source bean
     * @param target          – the target bean
     * @param ignoreNullValue whether ignore null value properties in source bean
     */
    public static void copyProperties(Object source, Object target, boolean ignoreNullValue) {
        if (source == null) {
            throw new IllegalArgumentException("source object is null");
        }

        if (target == null) {
            throw new IllegalArgumentException("target object is null");
        }

        try {
            Map<String, PropertyDescriptor> sourcePds = getPropertyDescriptors(source);
            Map<String, PropertyDescriptor> targetPds = getPropertyDescriptors(target);

            sourcePds.forEach((name, sourcePd) -> {
                PropertyDescriptor targetPd = targetPds.get(name);
                if (sourcePd.getPropertyType().equals(targetPd.getPropertyType())
                        && targetPd.getWriteMethod() != null) {
                    try {
                        Object value = sourcePd.getReadMethod().invoke(source);
                        if (value != null || !ignoreNullValue) {
                            targetPd.getWriteMethod().invoke(target, value);
                        }
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static Map<String, PropertyDescriptor> getPropertyDescriptors(Object object) {
        try {
            return Arrays.stream(Introspector.getBeanInfo(object.getClass()).getPropertyDescriptors()).collect(
                    Collectors.toMap(PropertyDescriptor::getName, v -> v, (key1, key2) -> key2));
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        }
    }
}