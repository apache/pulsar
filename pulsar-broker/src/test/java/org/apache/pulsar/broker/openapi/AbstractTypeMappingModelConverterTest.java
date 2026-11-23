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
import com.fasterxml.jackson.annotation.JsonView;
import io.swagger.v3.core.converter.AnnotatedType;
import io.swagger.v3.core.converter.ModelConverters;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.media.Schema;
import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.function.Function;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AbstractTypeMappingModelConverterTest {

    private static Map<String, Schema> schemas(Class<?> type) {
        ModelConverters converters = new ModelConverters();
        converters.addConverter(new AbstractTypeMappingModelConverter());
        return converters.readAll(new AnnotatedType(type));
    }

    public void testInterfaceIsDescribedWithImplementationJsonNames() {
        Map<String, Schema> schemas = schemas(NamespaceIsolationData.class);

        assertThat(schemas.get("NamespaceIsolationData").getProperties())
                .containsKeys("auto_failover_policy", "unload_scope")
                .doesNotContainKeys("autoFailoverPolicy", "unloadScope");
        assertThat(schemas.get("AutoFailoverPolicyData").getProperties())
                .containsKey("policy_type")
                .doesNotContainKey("policyType");
    }

    public void testInterfaceIsDescribedWithImplementationOnlyProperties() {
        Map<String, Schema> schemas = schemas(TopicStats.class);

        assertThat(schemas.get("TopicStats").getProperties()).containsKeys("abortedTxnCount", "ongoingTxnCount");
        // TopicStatsImpl declares its subscriptions as a Map<String, SubscriptionStatsImpl>
        assertThat(schemas.get("SubscriptionStats").getProperties()).containsKeys("isDurable", "msgExpired");
        // TopicStatsImpl declares its compaction field as CompactionStatsImpl
        assertThat(schemas).containsKey("CompactionStats");
        assertThat(schemas.keySet()).noneMatch(name -> name.endsWith("Impl"));
    }

    public void testImplementationIsNamedAfterItsInterface() {
        Map<String, Schema> schemas = schemas(FunctionInstanceStatsImpl.class);

        assertThat(schemas).containsOnlyKeys("FunctionInstanceStats", "FunctionInstanceStatsData",
                "FunctionInstanceStatsDataBase");
        assertThat(schemas.get("FunctionInstanceStatsData").getProperties())
                .containsKey("1min")
                .doesNotContainKey("oneMin");
    }

    public void testImplementationKeepsAnnotatedTypeState() throws Exception {
        AnnotatedType defaults = new AnnotatedType();
        Annotation[] ctxAnnotations = AbstractTypeMappingModelConverterTest.class.getAnnotations();
        JsonView jsonView = AbstractTypeMappingModelConverterTest.class.getDeclaredMethod("jsonViewHolder")
                .getAnnotation(JsonView.class);
        Function<AnnotatedType, Schema> jsonUnwrappedHandler = type -> null;
        Schema<?> parent = new Schema<>();
        Components components = new Components();
        AnnotatedType declared = new AnnotatedType(TopicStats.class)
                .ctxAnnotations(ctxAnnotations)
                .jsonViewAnnotation(jsonView)
                .includePropertiesWithoutJSONView(!defaults.isIncludePropertiesWithoutJSONView())
                .jsonUnwrappedHandler(jsonUnwrappedHandler)
                .schemaProperty(!defaults.isSchemaProperty())
                .resolveAsRef(!defaults.isResolveAsRef())
                .resolveEnumAsRef(!defaults.isResolveEnumAsRef())
                .skipOverride(!defaults.isSkipOverride())
                .skipJsonIdentity(!defaults.isSkipJsonIdentity())
                .skipSchemaName(!defaults.isSkipSchemaName())
                .subtype(!defaults.isSubtype())
                .parent(parent)
                .propertyName("stats")
                .components(components);

        AnnotatedType described = new AbstractTypeMappingModelConverter().mapToImplementation(declared);

        assertThat(described.getType()).isEqualTo(TopicStatsImpl.class);
        assertThat(described.getName()).isEqualTo("TopicStats");
        assertThat(described.getCtxAnnotations()).isNotEmpty().containsExactly(ctxAnnotations);
        assertThat(described.getJsonViewAnnotation()).isSameAs(jsonView);
        assertThat(described.isIncludePropertiesWithoutJSONView())
                .isEqualTo(declared.isIncludePropertiesWithoutJSONView());
        assertThat(described.getJsonUnwrappedHandler()).isSameAs(jsonUnwrappedHandler);
        assertThat(described.isSchemaProperty()).isEqualTo(declared.isSchemaProperty());
        assertThat(described.isResolveAsRef()).isEqualTo(declared.isResolveAsRef());
        assertThat(described.isResolveEnumAsRef()).isEqualTo(declared.isResolveEnumAsRef());
        assertThat(described.isSkipOverride()).isEqualTo(declared.isSkipOverride());
        assertThat(described.isSkipJsonIdentity()).isEqualTo(declared.isSkipJsonIdentity());
        assertThat(described.isSkipSchemaName()).isEqualTo(declared.isSkipSchemaName());
        assertThat(described.isSubtype()).isEqualTo(declared.isSubtype());
        assertThat(described.getParent()).isSameAs(parent);
        assertThat(described.getPropertyName()).isEqualTo("stats");
        assertThat(described.getComponents()).isSameAs(components);
    }

    @JsonView(Object.class)
    private static void jsonViewHolder() {
    }

    public void testOtherTypesAreUnchanged() {
        AbstractTypeMappingModelConverter converter = new AbstractTypeMappingModelConverter();
        AnnotatedType unmapped = new AnnotatedType(String.class);
        AnnotatedType named = new AnnotatedType(TopicStats.class).name("CustomName");

        assertThat(converter.mapToImplementation(unmapped)).isSameAs(unmapped);
        assertThat(converter.mapToImplementation(named)).isSameAs(named);
    }
}
