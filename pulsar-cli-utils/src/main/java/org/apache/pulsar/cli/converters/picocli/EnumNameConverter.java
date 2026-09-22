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
package org.apache.pulsar.cli.converters.picocli;

import java.util.Arrays;
import java.util.Locale;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.TypeConversionException;

/**
 * Converts an enum constant name, ignoring case and underscores, so that both {@code ExclusiveWithFencing}
 * and {@code EXCLUSIVE_WITH_FENCING} select the same constant.
 *
 * <p>picocli instantiates converters through a no-argument constructor, so subclass this once per enum.
 */
public abstract class EnumNameConverter<E extends Enum<E>> implements ITypeConverter<E> {
    private final Class<E> type;

    protected EnumNameConverter(Class<E> type) {
        this.type = type;
    }

    @Override
    public E convert(String value) {
        String wanted = normalize(value);
        for (E constant : type.getEnumConstants()) {
            if (normalize(constant.name()).equals(wanted)) {
                return constant;
            }
        }
        throw new TypeConversionException("expected one of " + Arrays.toString(type.getEnumConstants())
                + " but was '" + value + "'");
    }

    /**
     * Maps a constant to the constant of another enum whose name matches it, ignoring case and underscores.
     */
    public static <T extends Enum<T>> T mapByName(Enum<?> value, Class<T> targetType) {
        if (value == null) {
            return null;
        }
        String wanted = normalize(value.name());
        for (T constant : targetType.getEnumConstants()) {
            if (normalize(constant.name()).equals(wanted)) {
                return constant;
            }
        }
        throw new IllegalArgumentException("No " + targetType.getSimpleName() + " constant matches " + value);
    }

    private static String normalize(String name) {
        return name.replace("_", "").toLowerCase(Locale.ROOT);
    }
}
