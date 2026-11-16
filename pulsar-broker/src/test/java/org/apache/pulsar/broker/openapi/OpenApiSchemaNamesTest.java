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

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.reflect.ClassPath;
import io.swagger.v3.core.converter.AnnotatedType;
import io.swagger.v3.core.converter.ModelConverter;
import io.swagger.v3.core.converter.ModelConverterContext;
import io.swagger.v3.core.converter.ModelConverters;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.jaxrs2.Reader;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import jakarta.ws.rs.Path;
import java.io.IOException;
import java.security.CodeSource;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.v2.Namespaces;
import org.apache.pulsar.broker.admin.v2.PersistentTopics;
import org.apache.pulsar.broker.admin.v3.Functions;
import org.apache.pulsar.broker.lookup.v2.TopicLookup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Checks that no two classes share a schema name in the OpenAPI specifications. Swagger names a schema after the
 * simple name of its class and keeps a single schema per name, so a reference to any other class with that name
 * silently points to the wrong schema.
 *
 * <p>The specifications are read from the same REST resources as the OpenAPI tasks in
 * {@code pulsar-broker/build.gradle.kts}: all admin v2 resources in one specification, and each admin v3 and
 * lookup resource in its own.
 */
@Test(groups = "broker")
public class OpenApiSchemaNamesTest {
    private final AbstractTypeMappingModelConverter abstractTypeMappings = new AbstractTypeMappingModelConverter();
    private final SchemaNameRecorder recorder = new SchemaNameRecorder();

    @BeforeClass(alwaysRun = true)
    public void registerConverters() {
        // Swagger's Reader uses the shared ModelConverters instance; addConverter puts each converter first
        ModelConverters.getInstance().addConverter(recorder);
        ModelConverters.getInstance().addConverter(abstractTypeMappings);
    }

    @AfterClass(alwaysRun = true)
    public void removeConverters() {
        ModelConverters.getInstance().removeConverter(abstractTypeMappings);
        ModelConverters.getInstance().removeConverter(recorder);
    }

    @DataProvider
    public Object[][] specifications() throws IOException {
        ClassPath classPath = ClassPath.from(OpenApiSchemaNamesTest.class.getClassLoader());
        Set<Class<?>> adminV2 = resources(classPath, "org.apache.pulsar.broker.admin.v2");
        Set<Class<?>> separate = new TreeSet<>(Comparator.comparing(Class::getName));
        separate.addAll(resources(classPath, "org.apache.pulsar.broker.admin.v3"));
        separate.addAll(resources(classPath, "org.apache.pulsar.broker.lookup.v2"));
        assertThat(adminV2).contains(Namespaces.class, PersistentTopics.class);
        assertThat(separate).contains(Functions.class, TopicLookup.class);
        Object[][] specifications = new Object[separate.size() + 1][];
        specifications[0] = new Object[] {"admin v2", adminV2};
        int i = 1;
        for (Class<?> resource : separate) {
            specifications[i++] = new Object[] {resource.getSimpleName(), Set.of(resource)};
        }
        return specifications;
    }

    /**
     * Returns the REST resources of a package in the broker's main classes, leaving out those of its tests.
     */
    private static Set<Class<?>> resources(ClassPath classPath, String packageName) {
        CodeSource mainClasses = PulsarService.class.getProtectionDomain().getCodeSource();
        return classPath.getTopLevelClasses(packageName).stream()
                .map(ClassPath.ClassInfo::load)
                .filter(cls -> cls.isAnnotationPresent(Path.class))
                .filter(cls -> mainClasses.equals(cls.getProtectionDomain().getCodeSource()))
                .collect(Collectors.toSet());
    }

    @Test(dataProvider = "specifications")
    public void testNoSchemaNameIsSharedByTwoClasses(String specification, Set<Class<?>> resources) {
        recorder.classesBySchemaName.clear();

        new Reader(new OpenAPI()).read(resources);

        assertThat(recorder.classesBySchemaName).as("schemas recorded for %s", specification).isNotEmpty();
        Map<String, Set<String>> shared = new TreeMap<>(recorder.classesBySchemaName);
        shared.values().removeIf(classes -> classes.size() < 2);
        assertThat(shared).as("schema names shared by several classes in %s", specification).isEmpty();
    }

    /**
     * Records the classes that Swagger describes under each schema name. It runs after
     * {@link AbstractTypeMappingModelConverter}, so it sees the classes that are actually described.
     */
    private static class SchemaNameRecorder implements ModelConverter {
        private final Map<String, Set<String>> classesBySchemaName = new HashMap<>();

        @Override
        public Schema resolve(AnnotatedType annotatedType, ModelConverterContext context,
                              Iterator<ModelConverter> chain) {
            JavaType type = Json.mapper().constructType(annotatedType.getType());
            Class<?> cls = type.getRawClass();
            if (cls.getName().startsWith("org.apache.pulsar.") && !cls.isEnum() && !type.isContainerType()
                    && !type.hasGenericTypes()) {
                classesBySchemaName.computeIfAbsent(schemaName(annotatedType, cls), name -> new TreeSet<>())
                        .add(cls.getName());
            }
            return chain.hasNext() ? chain.next().resolve(annotatedType, context, chain) : null;
        }

        private static String schemaName(AnnotatedType annotatedType, Class<?> cls) {
            if (annotatedType.getName() != null) {
                return annotatedType.getName();
            }
            io.swagger.v3.oas.annotations.media.Schema schema =
                    cls.getAnnotation(io.swagger.v3.oas.annotations.media.Schema.class);
            return schema != null && StringUtils.isNotBlank(schema.name()) ? schema.name() : cls.getSimpleName();
        }
    }
}
