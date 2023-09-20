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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.ConstructorDetector;
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.cfg.HandlerInstantiator;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.introspect.AccessorNamingStrategy;
import com.fasterxml.jackson.databind.introspect.ClassIntrospector;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.text.DateFormat;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/**
 * Jackson ObjectMapper which creates an ObjectMapper instance that is protected against
 * mutating the configuration.
 *
 * A preferred approach is to depend on {@link com.fasterxml.jackson.databind.ObjectWriter} and
 * {@link com.fasterxml.jackson.databind.ObjectReader} instances instead of the {@link ObjectMapper} since
 * those classes are immutable.
 */
final class ProtectedObjectMapper extends ObjectMapper {

    private final ObjectMapper src;

    static ObjectMapper protectedCopyOf(ObjectMapper src) {
        return new ProtectedObjectMapper(src);
    }

    private ProtectedObjectMapper(ObjectMapper src) {
        super(src);
        this.src = src;
    }

    @Override
    public ObjectMapper copy() {
        return src.copy();
    }

    private static UnsupportedOperationException createUnsupportedOperationException() {
        return new UnsupportedOperationException("Modifying configuration isn't supported. "
                + "Make a copy of this instance with .copy() and configure that instance.");
    }

    @Override
    public ObjectMapper configure(MapperFeature f, boolean state) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper configure(SerializationFeature f, boolean state) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper configure(DeserializationFeature f, boolean state) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper configure(JsonParser.Feature f, boolean state) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper configure(JsonGenerator.Feature f, boolean state) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setSerializerFactory(SerializerFactory f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setSerializerProvider(DefaultSerializerProvider p) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setMixIns(Map<Class<?>, Class<?>> sourceMixins) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper addMixIn(Class<?> target, Class<?> mixinSource) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setMixInResolver(ClassIntrospector.MixInResolver resolver) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setVisibility(VisibilityChecker<?> vc) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setVisibility(PropertyAccessor forMethod, JsonAutoDetect.Visibility visibility) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setSubtypeResolver(SubtypeResolver str) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setAnnotationIntrospector(AnnotationIntrospector ai) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setAnnotationIntrospectors(AnnotationIntrospector serializerAI,
                                                   AnnotationIntrospector deserializerAI) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setPropertyNamingStrategy(PropertyNamingStrategy s) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setAccessorNaming(AccessorNamingStrategy.Provider s) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setDefaultPrettyPrinter(PrettyPrinter pp) {
        throw createUnsupportedOperationException();
    }

    @Override
    public void setVisibilityChecker(VisibilityChecker<?> vc) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setPolymorphicTypeValidator(PolymorphicTypeValidator ptv) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setSerializationInclusion(JsonInclude.Include incl) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setPropertyInclusion(JsonInclude.Value incl) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setDefaultPropertyInclusion(JsonInclude.Value incl) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setDefaultPropertyInclusion(JsonInclude.Include incl) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setDefaultSetterInfo(JsonSetter.Value v) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setDefaultVisibility(JsonAutoDetect.Value vis) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setDefaultMergeable(Boolean b) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setDefaultLeniency(Boolean b) {
        throw createUnsupportedOperationException();
    }

    @Override
    public void registerSubtypes(Class<?>... classes) {
        throw createUnsupportedOperationException();
    }

    @Override
    public void registerSubtypes(NamedType... types) {
        throw createUnsupportedOperationException();
    }

    @Override
    public void registerSubtypes(Collection<Class<?>> subtypes) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper activateDefaultTyping(PolymorphicTypeValidator ptv) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper activateDefaultTyping(PolymorphicTypeValidator ptv, DefaultTyping applicability) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper activateDefaultTyping(PolymorphicTypeValidator ptv, DefaultTyping applicability,
                                              JsonTypeInfo.As includeAs) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper activateDefaultTypingAsProperty(PolymorphicTypeValidator ptv, DefaultTyping applicability,
                                                        String propertyName) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper deactivateDefaultTyping() {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setDefaultTyping(TypeResolverBuilder<?> typer) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enableDefaultTyping() {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enableDefaultTyping(DefaultTyping dti) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enableDefaultTyping(DefaultTyping applicability, JsonTypeInfo.As includeAs) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enableDefaultTypingAsProperty(DefaultTyping applicability, String propertyName) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper disableDefaultTyping() {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setTypeFactory(TypeFactory f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setNodeFactory(JsonNodeFactory f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setConstructorDetector(ConstructorDetector cd) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper addHandler(DeserializationProblemHandler h) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper clearProblemHandlers() {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setConfig(DeserializationConfig config) {
        throw createUnsupportedOperationException();
    }

    @Override
    public void setFilters(FilterProvider filterProvider) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setFilterProvider(FilterProvider filterProvider) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setBase64Variant(Base64Variant v) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setConfig(SerializationConfig config) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setDateFormat(DateFormat dateFormat) {
        throw createUnsupportedOperationException();
    }

    @Override
    public Object setHandlerInstantiator(HandlerInstantiator hi) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setInjectableValues(InjectableValues injectableValues) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setLocale(Locale l) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setTimeZone(TimeZone tz) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper setDefaultAttributes(ContextAttributes attrs) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enable(MapperFeature... f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper disable(MapperFeature... f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enable(SerializationFeature f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enable(SerializationFeature first, SerializationFeature... f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper disable(SerializationFeature f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper disable(SerializationFeature first, SerializationFeature... f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enable(DeserializationFeature feature) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enable(DeserializationFeature first, DeserializationFeature... f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper disable(DeserializationFeature feature) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper disable(DeserializationFeature first, DeserializationFeature... f) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enable(JsonParser.Feature... features) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper disable(JsonParser.Feature... features) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper enable(JsonGenerator.Feature... features) {
        throw createUnsupportedOperationException();
    }

    @Override
    public ObjectMapper disable(JsonGenerator.Feature... features) {
        throw createUnsupportedOperationException();
    }
}
