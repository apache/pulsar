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
package org.apache.pulsar.client.impl.schema;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Random;
import org.apache.avro.util.ClassSecurityValidator;
import org.apache.avro.util.ClassSecurityValidator.ClassSecurityPredicate;
import org.apache.pulsar.common.events.PulsarEvent;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests the trusted-class rules mostly through {@link PulsarAvroClassSecurity#isTrusted(String, String)}
 * rather than through Avro's global validator, because the Gradle test JVM already trusts the whole
 * org.apache.pulsar namespace (see pulsar.java-conventions.gradle.kts) and would mask them.
 *
 * <p>The tests that do touch the global validator only ever widen it and restore it afterwards, since it
 * is JVM-global and other test classes may be decoding Avro in the same fork.
 */
public class PulsarAvroClassSecurityTest {

    private ClassSecurityPredicate previousValidator;

    @BeforeMethod
    public void saveValidator() {
        previousValidator = ClassSecurityValidator.getGlobal();
    }

    @AfterMethod(alwaysRun = true)
    public void restoreValidator() {
        ClassSecurityValidator.setGlobal(previousValidator);
    }

    @Test
    public void testPulsarInternalAvroTypesAreTrusted() {
        // One case per trusted package/class entry, covering the types Pulsar serializes with Avro.
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.metadata.api.MetadataEvent", "org.apache.pulsar.metadata.api")).isTrue();
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.metadata.api.NotificationType", "org.apache.pulsar.metadata.api")).isTrue();
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.metadata.api.extended.CreateOption",
                "org.apache.pulsar.metadata.api.extended")).isTrue();
        // Collection types emitted as a "java-class" property on a generated array or map schema:
        // MetadataEvent.options is a HashSet, TransactionBufferSnapshot.aborts is a List.
        assertThat(PulsarAvroClassSecurity.isTrusted("java.util.HashSet", "java.util")).isTrue();
        assertThat(PulsarAvroClassSecurity.isTrusted("java.util.List", "java.util")).isTrue();
        assertThat(PulsarAvroClassSecurity.isTrusted("java.util.Map", "java.util")).isTrue();
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.common.events.PulsarEvent", "org.apache.pulsar.common.events")).isTrue();
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.common.policies.data.TopicPolicies",
                "org.apache.pulsar.common.policies.data")).isTrue();
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.broker.transaction.buffer.metadata.TransactionBufferSnapshot",
                "org.apache.pulsar.broker.transaction.buffer.metadata")).isTrue();
        assertThat(PulsarAvroClassSecurity.isTrusted("com.google.protobuf.Any", "com.google.protobuf")).isTrue();
    }

    @Test
    public void testSubPackagesOfTrustedPackagesAreTrusted() {
        // The .v2 snapshot format and the policies .impl types are reached by sub-package matching
        // rather than by their own entries.
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotSegment",
                "org.apache.pulsar.broker.transaction.buffer.metadata.v2")).isTrue();
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl",
                "org.apache.pulsar.common.policies.data.impl")).isTrue();
    }

    @Test
    public void testNestedClassesAreMatchedByTheirPackage() {
        // SpecificData.getClass retries with '$' separators, so the validated name is the binary name
        // while the package stays that of the enclosing class.
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.common.events.PulsarEvent$Inner", "org.apache.pulsar.common.events")).isTrue();
    }

    @Test
    public void testPackageMatchingRequiresASeparatorAtTheBoundary() {
        // A package that merely shares a prefix with a trusted one must not be trusted.
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.common.eventsEvil.Gadget", "org.apache.pulsar.common.eventsEvil")).isFalse();
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "com.google.protobufEvil.Gadget", "com.google.protobufEvil")).isFalse();
    }

    @Test
    public void testUntrustedClassesAreRejected() {
        // Application POJOs stay untrusted; they are the application's own to allow.
        assertThat(PulsarAvroClassSecurity.isTrusted("com.example.MyPojo", "com.example")).isFalse();
        // Only the collection types Pulsar's own records declare are trusted, not java.util wholesale,
        // so other JDK classes in the same package stay untrusted.
        assertThat(PulsarAvroClassSecurity.isTrusted("java.util.Random", "java.util")).isFalse();
        assertThat(PulsarAvroClassSecurity.isTrusted("java.util.TreeMap", "java.util")).isFalse();
        // Pulsar packages that are not Avro-serialized are not blanket-trusted either.
        assertThat(PulsarAvroClassSecurity.isTrusted(
                "org.apache.pulsar.client.impl.ConsumerImpl", "org.apache.pulsar.client.impl")).isFalse();
    }

    @Test
    public void testDefaultPackageIsNeverTrusted() {
        assertThat(PulsarAvroClassSecurity.isTrusted("Gadget", null)).isFalse();
    }

    @Test
    public void testInstallComposesWithTheCurrentValidatorRatherThanReplacingIt() {
        // Widen the current validator with a marker, so that finding the marker still trusted after
        // install() proves install() composed rather than replaced.
        ClassSecurityPredicate marker = clazz -> clazz.getName().equals(ApplicationPojo.class.getName());
        ClassSecurityPredicate baseline =
                ClassSecurityValidator.composite(ClassSecurityValidator.getGlobal(), marker);
        ClassSecurityValidator.setGlobal(baseline);

        PulsarAvroClassSecurity.install();

        ClassSecurityPredicate installed = ClassSecurityValidator.getGlobal();
        assertThat(installed).isNotSameAs(baseline);
        assertThat(installed.isTrusted(ApplicationPojo.class)).isTrue();
        assertThat(installed.isTrusted(PulsarEvent.class)).isTrue();
    }

    @Test
    public void testInstallIsIdempotent() {
        ClassSecurityValidator.setGlobal(
                ClassSecurityValidator.composite(ClassSecurityValidator.getGlobal(), clazz -> false));
        PulsarAvroClassSecurity.install();
        ClassSecurityPredicate afterFirst = ClassSecurityValidator.getGlobal();

        PulsarAvroClassSecurity.install();

        assertThat(ClassSecurityValidator.getGlobal()).isSameAs(afterFirst);
    }

    @Test
    public void testInstallReappliesAfterTheGlobalValidatorWasRestored() {
        PulsarAvroClassSecurity.install();
        // A test restoring an earlier global value must not leave install() believing it is still active.
        ClassSecurityPredicate restored =
                ClassSecurityValidator.composite(ClassSecurityValidator.getGlobal(), clazz -> false);
        ClassSecurityValidator.setGlobal(restored);

        PulsarAvroClassSecurity.install();

        assertThat(ClassSecurityValidator.getGlobal()).isNotSameAs(restored);
        assertThat(ClassSecurityValidator.getGlobal().isTrusted(PulsarEvent.class)).isTrue();
    }

    @Test
    public void testTrustAdditionallyIsUndoneByRestoringTheReturnedValidator() {
        // Random is trusted neither by Avro's defaults, nor by Pulsar, nor by the test system property.
        ClassSecurityPredicate before = ClassSecurityValidator.getGlobal();
        assertThat(before.isTrusted(Random.class)).isFalse();

        ClassSecurityPredicate returned = PulsarAvroClassSecurity.trustAdditionally(Random.class);

        assertThat(returned).isSameAs(before);
        assertThat(ClassSecurityValidator.getGlobal().isTrusted(Random.class)).isTrue();

        PulsarAvroClassSecurity.restoreGlobal(returned);
        assertThat(ClassSecurityValidator.getGlobal().isTrusted(Random.class)).isFalse();
    }

    /** Stand-in for an application class that Pulsar does not trust on its own. */
    static class ApplicationPojo {
    }
}
