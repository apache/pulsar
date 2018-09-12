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
package org.apache.pulsar.functions.instance;

import static com.google.common.base.Preconditions.checkArgument;

import lombok.experimental.UtilityClass;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.utils.Reflections;

import net.jodah.typetools.TypeResolver;

@UtilityClass
public class InstanceUtils {
    public static SerDe<?> initializeSerDe(String serdeClassName, ClassLoader clsLoader, Class<?> typeArg,
                                           boolean deser) {
        SerDe<?> serDe = createInstance(serdeClassName, clsLoader, SerDe.class);

        Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());
        if (deser) {
            checkArgument(typeArg.isAssignableFrom(inputSerdeTypeArgs[0]),
                    "Inconsistent types found between function input type and serde type: "
                            + " function type = " + typeArg + " should be assignable from "
                            + inputSerdeTypeArgs[0]);
        } else {
            checkArgument(inputSerdeTypeArgs[0].isAssignableFrom(typeArg),
                    "Inconsistent types found between function input type and serde type: "
                            + " serde type = " + inputSerdeTypeArgs[0] + " should be assignable from "
                            + typeArg);
        }

        return serDe;
    }

    public static Schema<?> initializeCustomSchema(String schemaClassName, ClassLoader clsLoader, Class<?> typeArg,
                                                   boolean input) {
        Schema<?> schema = createInstance(schemaClassName, clsLoader, Schema.class);

        Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(Schema.class, schema.getClass());
        if (input) {
            checkArgument(typeArg.isAssignableFrom(inputSerdeTypeArgs[0]),
                    "Inconsistent types found between function type and schema type: "
                            + " function type = " + typeArg + " should be assignable from "
                            + inputSerdeTypeArgs[0]);
        } else {
            checkArgument(inputSerdeTypeArgs[0].isAssignableFrom(typeArg),
                    "Inconsistent types found between function type and schema type: "
                            + " schema type = " + inputSerdeTypeArgs[0] + " should be assignable from "
                            + typeArg);
        }
        return schema;
    }

    private static <T> T createInstance(String className, ClassLoader clsLoader, Class<T> baseClass) {
        if (StringUtils.isEmpty(className)) {
            return null;
        } else {
            return Reflections.createInstance(className, baseClass, clsLoader);
        }
    }
}
