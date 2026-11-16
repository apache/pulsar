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
package org.apache.pulsar.broker.openapi;

import com.fasterxml.jackson.databind.JavaType;
import io.swagger.v3.core.converter.AnnotatedType;
import io.swagger.v3.core.converter.ModelConverter;
import io.swagger.v3.core.converter.ModelConverterContext;
import io.swagger.v3.oas.models.media.Schema;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Makes the OpenAPI specification describe the JSON that the REST API actually exchanges for
 * interface-typed values.
 *
 * <p>The REST resources and data classes declare many values as an interface, such as {@code TopicStats},
 * while the broker serializes an implementation, such as {@code TopicStatsImpl}, and deserializes one through
 * the abstract type mappings of {@link ObjectMapperFactory}. Swagger describes the declared interface, so the
 * specification misses the JSON names and the properties that only the implementation declares. This converter
 * replaces each mapped interface with its implementation before the default resolver describes it, using the
 * same mappings as {@link ObjectMapperFactory#getAbstractTypeMappings()}.
 *
 * <p>The schema keeps the interface's simple name, also when a resource or a field refers to the
 * implementation directly, so that one schema describes both.
 *
 * <p>The OpenAPI generation tasks in {@code pulsar-broker/build.gradle.kts} register this converter through
 * the swagger-gradle-plugin's {@code modelConverterClasses}. It is not part of the broker's runtime.
 */
public class AbstractTypeMappingModelConverter implements ModelConverter {
    private final Map<Class<?>, Class<?>> implementations;
    private final Map<Class<?>, String> schemaNames;

    /**
     * Creates a converter that uses the abstract type mappings of {@link ObjectMapperFactory}.
     */
    public AbstractTypeMappingModelConverter() {
        this(ObjectMapperFactory.getAbstractTypeMappings());
    }

    AbstractTypeMappingModelConverter(Map<Class<?>, Class<?>> implementations) {
        this.implementations = Map.copyOf(implementations);
        Map<Class<?>, String> names = new HashMap<>();
        implementations.forEach((abstractType, implementation) -> {
            names.put(abstractType, abstractType.getSimpleName());
            names.put(implementation, abstractType.getSimpleName());
        });
        this.schemaNames = Map.copyOf(names);
    }

    @Override
    public Schema resolve(AnnotatedType annotatedType, ModelConverterContext context,
                          Iterator<ModelConverter> chain) {
        if (!chain.hasNext()) {
            return null;
        }
        return chain.next().resolve(mapToImplementation(annotatedType), context, chain);
    }

    /**
     * Returns the type to describe in place of the given one: the implementation of a mapped interface, named
     * after the interface. Returns the given type unchanged when it is neither a mapped interface nor its
     * implementation, or when its schema name is already set.
     */
    AnnotatedType mapToImplementation(AnnotatedType annotatedType) {
        // Swagger's ModelResolver resolves the type it gets again, through this converter too, so the
        // returned type, which has a name, must come back unchanged.
        if (annotatedType.getName() != null) {
            return annotatedType;
        }
        Class<?> declaredClass = nonGenericClass(annotatedType.getType());
        if (declaredClass == null) {
            return annotatedType;
        }
        String schemaName = schemaNames.get(declaredClass);
        if (schemaName == null) {
            return annotatedType;
        }
        Class<?> describedClass = implementations.getOrDefault(declaredClass, declaredClass);
        return new AnnotatedType(describedClass)
                .name(schemaName)
                .ctxAnnotations(annotatedType.getCtxAnnotations())
                .jsonViewAnnotation(annotatedType.getJsonViewAnnotation())
                .includePropertiesWithoutJSONView(annotatedType.isIncludePropertiesWithoutJSONView())
                .jsonUnwrappedHandler(annotatedType.getJsonUnwrappedHandler())
                .schemaProperty(annotatedType.isSchemaProperty())
                .resolveAsRef(annotatedType.isResolveAsRef())
                .resolveEnumAsRef(annotatedType.isResolveEnumAsRef())
                .skipOverride(annotatedType.isSkipOverride())
                .skipJsonIdentity(annotatedType.isSkipJsonIdentity())
                .skipSchemaName(annotatedType.isSkipSchemaName())
                .subtype(annotatedType.isSubtype())
                .parent(annotatedType.getParent())
                .propertyName(annotatedType.getPropertyName())
                .components(annotatedType.getComponents());
    }

    /**
     * Returns the class of a type that is a plain class, such as {@code TopicStats}, or {@code null} for any other
     * type, such as a parameterized or container type. The mapped interfaces are not generic, and Swagger
     * resolves the elements of a container type, such as {@code List<TopicStats>}, as types of their own.
     */
    private static Class<?> nonGenericClass(Type type) {
        if (type instanceof Class<?> cls) {
            return cls;
        }
        if (type instanceof JavaType javaType && !javaType.isContainerType() && !javaType.hasGenericTypes()) {
            return javaType.getRawClass();
        }
        return null;
    }
}
