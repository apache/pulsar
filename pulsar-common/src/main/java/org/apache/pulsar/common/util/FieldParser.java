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
package org.apache.pulsar.common.util;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.util.EnumResolver;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Generic value converter.
 *
 * <p><h3>Use examples</h3>
 *
 * <pre>
 * String o1 = String.valueOf(1);
 * ;
 * Integer i = FieldParser.convert(o1, Integer.class);
 * System.out.println(i); // 1
 *
 * </pre>
 *
 */
public final class FieldParser {

    private static final Map<String, Method> CONVERTERS = new HashMap<>();
    private static final Map<Class<?>, Class<?>> WRAPPER_TYPES = new HashMap<>();

    private static final AnnotationIntrospector ANNOTATION_INTROSPECTOR = new JacksonAnnotationIntrospector();

    static {
        // Preload converters and wrapperTypes.
        initConverters();
        initWrappers();
    }

    /**
     * Convert the given object value to the given class.
     *
     * @param from
     *            The object value to be converted.
     * @param to
     *            The type class which the given object should be converted to.
     * @return The converted object value.
     * @throws UnsupportedOperationException
     *             If no suitable converter can be found.
     * @throws RuntimeException
     *             If conversion failed somehow. This can be caused by at least an ExceptionInInitializerError,
     *             IllegalAccessException or InvocationTargetException.
     */
    @SuppressWarnings("unchecked")
    public static <T> T convert(Object from, Class<T> to) {

        requireNonNull(to);
        if (from == null) {
            return null;
        }

        to = (Class<T>) wrap(to);
        // Can we cast? Then just do it.
        if (to.isAssignableFrom(from.getClass())) {
            return to.cast(from);
        }

        // Lookup the suitable converter.
        String converterId = from.getClass().getName() + "_" + to.getName();
        Method converter = CONVERTERS.get(converterId);

        if (to.isEnum()) {
            // Converting string to enum
            EnumResolver r = EnumResolver.constructUsingToString((Class<Enum<?>>) to, ANNOTATION_INTROSPECTOR);
            T value = (T) r.findEnum((String) from);
            if (value == null) {
                throw new RuntimeException("Invalid value '" + from + "' for enum " + to);
            }
            return value;
        }

        if (converter == null) {
            throw new UnsupportedOperationException("Cannot convert from " + from.getClass().getName() + " to "
                    + to.getName() + ". Requested converter does not exist.");
        }

        // Convert the value.
        try {
            Object val = converter.invoke(to, from);
            return to.cast(val);
        } catch (Exception e) {
            throw new RuntimeException("Cannot convert from " + from.getClass().getName() + " to " + to.getName()
                    + ". Conversion failed with " + e.getMessage(), e);
        }
    }

    /**
     * Update given Object attribute by reading it from provided map properties.
     *
     * @param properties
     *            which key-value pair of properties to assign those values to given object
     * @param obj
     *            object which needs to be updated
     * @throws IllegalArgumentException
     *             if the properties key-value contains incorrect value type
     */
    public static <T> void update(Map<String, String> properties, T obj) throws IllegalArgumentException {
        Field[] fields = obj.getClass().getDeclaredFields();
        Arrays.stream(fields).forEach(f -> {
            if (properties.containsKey(f.getName())) {
                try {
                    f.setAccessible(true);
                    String v = properties.get(f.getName());
                    if (!StringUtils.isBlank(v)) {
                        f.set(obj, value(trim(v), f));
                    } else {
                        setEmptyValue(v, f, obj);
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(format("failed to initialize %s field while setting value %s",
                            f.getName(), properties.get(f.getName())), e);
                }
            }
        });
    }

    /**
     * Converts value as per appropriate DataType of the field.
     *
     * @param strValue
     *            : string value of the object
     * @param field
     *            : field of the attribute
     * @return
     */
    public static Object value(String strValue, Field field) {
        requireNonNull(field);
        // if field is not primitive type
        Type fieldType = field.getGenericType();
        if (fieldType instanceof ParameterizedType) {
            Class<?> clazz = (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
            if (field.getType().equals(List.class)) {
                // convert to list
                return stringToList(strValue, clazz);
            } else if (field.getType().equals(Set.class)) {
                // covert to set
                return stringToSet(strValue, clazz);
            } else if (field.getType().equals(Map.class)) {
                Class<?> valueClass =
                    (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];
                return stringToMap(strValue, clazz, valueClass);
            } else if (field.getType().equals(Optional.class)) {
                Type typeClazz = ((ParameterizedType) fieldType).getActualTypeArguments()[0];
                if (typeClazz instanceof ParameterizedType) {
                    throw new IllegalArgumentException(format("unsupported non-primitive Optional<%s> for %s",
                            typeClazz.getClass(), field.getName()));
                }
                return Optional.ofNullable(convert(strValue, (Class) typeClazz));
            } else {
                throw new IllegalArgumentException(
                        format("unsupported field-type %s for %s", field.getType(), field.getName()));
            }
        } else {
            return convert(strValue, field.getType());
        }
    }

    /**
     * Sets the empty/null value if field is allowed to be set empty.
     *
     * @param strValue
     * @param field
     * @param obj
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    public static <T> void setEmptyValue(String strValue, Field field, T obj)
            throws IllegalArgumentException, IllegalAccessException {
        requireNonNull(field);
        // if field is not primitive type
        Type fieldType = field.getGenericType();
        if (fieldType instanceof ParameterizedType) {
            if (field.getType().equals(List.class)) {
                field.set(obj, new ArrayList<>());
            } else if (field.getType().equals(Set.class)) {
                field.set(obj, new LinkedHashSet<>());
            } else if (field.getType().equals(Optional.class)) {
                field.set(obj, Optional.empty());
            } else {
                throw new IllegalArgumentException(
                        format("unsupported field-type %s for %s", field.getType(), field.getName()));
            }
        } else if (Number.class.isAssignableFrom(field.getType()) || fieldType.getClass().equals(String.class)) {
            field.set(obj, null);
        }
    }

    private static Class<?> wrap(Class<?> type) {
        return WRAPPER_TYPES.containsKey(type) ? WRAPPER_TYPES.get(type) : type;
    }

    private static void initConverters() {
        Method[] methods = FieldParser.class.getDeclaredMethods();
        Arrays.stream(methods).forEach(method -> {

            if (method.getParameterTypes().length == 1) {
                // Converter should accept 1 argument. This skips the convert() method.
                CONVERTERS.put(method.getParameterTypes()[0].getName() + "_" + method.getReturnType().getName(),
                        method);
            }
        });
    }

    private static void initWrappers() {
        WRAPPER_TYPES.put(int.class, Integer.class);
        WRAPPER_TYPES.put(float.class, Float.class);
        WRAPPER_TYPES.put(double.class, Double.class);
        WRAPPER_TYPES.put(long.class, Long.class);
        WRAPPER_TYPES.put(boolean.class, Boolean.class);
    }

    /***** --- Converters --- ****/

    /**
     * Converts String to Integer.
     *
     * @param val
     *            The String to be converted.
     * @return The converted Integer value.
     */
    public static Integer stringToInteger(String val) {
        String v = trim(val);
        if (io.netty.util.internal.StringUtil.isNullOrEmpty(v)) {
            return null;
        } else {
            return Integer.valueOf(v);
        }
    }

    /**
     * Converts String to Long.
     *
     * @param val
     *            The String to be converted.
     * @return The converted Long value.
     */
    public static Long stringToLong(String val) {
        return Long.valueOf(trim(val));
    }

    /**
     * Converts String to Double.
     *
     * @param val
     *            The String to be converted.
     * @return The converted Double value.
     */
    public static Double stringToDouble(String val) {
        String v = trim(val);
        if (io.netty.util.internal.StringUtil.isNullOrEmpty(v)) {
            return null;
        } else {
            return Double.valueOf(v);
        }
    }

    /**
     * Converts String to float.
     *
     * @param val
     *            The String to be converted.
     * @return The converted Double value.
     */
    public static Float stringToFloat(String val) {
        return Float.valueOf(trim(val));
    }

    /**
     * Converts comma separated string to List.
     *
     * @param <T>
     *            type of list
     * @param val
     *            comma separated values.
     * @return The converted list with type {@code <T>}.
     */
    public static <T> List<T> stringToList(String val, Class<T> type) {
        String[] tokens = trim(val).split(",");
        return Arrays.stream(tokens).map(t -> {
            return convert(trim(t), type);
        }).collect(Collectors.toList());
    }

    /**
     * Converts comma separated string to Set.
     *
     * @param <T>
     *            type of set
     * @param val
     *            comma separated values.
     * @return The converted set with type {@code <T>}.
     */
    public static <T> Set<T> stringToSet(String val, Class<T> type) {
        String[] tokens = trim(val).split(",");
        return Arrays.stream(tokens).map(t -> {
            return convert(trim(t), type);
        }).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static <K, V> Map<K, V> stringToMap(String strValue, Class<K> keyType, Class<V> valueType) {
        String[] tokens = trim(strValue).split(",");
        Map<K, V> map = new HashMap<>();
        for (String token : tokens) {
            String[] keyValue = trim(token).split("=");
            checkArgument(keyValue.length == 2,
                    strValue + " map-value is not in correct format key1=value,key2=value2");
            map.put(convert(trim(keyValue[0]), keyType), convert(trim(keyValue[1]), valueType));
        }
        return map;
    }

    private static String trim(String val) {
        requireNonNull(val);
        return val.trim();
    }

    /**
     * Converts Integer to String.
     *
     * @param value
     *            The Integer to be converted.
     * @return The converted String value.
     */
    public static String integerToString(Integer value) {
        return value.toString();
    }

    /**
     * Converts Boolean to String.
     *
     * @param value
     *            The Boolean to be converted.
     * @return The converted String value.
     */

    public static String booleanToString(Boolean value) {
        return value.toString();
    }

    /**
     * Converts String to Boolean.
     *
     * @param value
     *            The String to be converted.
     * @return The converted Boolean value.
     */
    public static Boolean stringToBoolean(String value) {
        return Boolean.valueOf(value);
    }

    // implement more converter methods here.

}
