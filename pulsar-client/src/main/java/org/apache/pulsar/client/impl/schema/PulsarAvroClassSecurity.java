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

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.avro.util.ClassSecurityValidator;
import org.apache.avro.util.ClassSecurityValidator.ClassSecurityPredicate;

/**
 * Grants Avro permission to reflect over the classes that Pulsar itself serializes with Avro.
 *
 * <p>Avro 1.12.2 (AVRO-4189) moved the trusted-class check into {@code ClassUtils.forName}, so every
 * reflective class resolution is now denied unless the class is allow-listed. Before that, in both
 * 1.12.0 and 1.12.1, the check applied only to classes named by the {@code java-class} /
 * {@code java-key-class} schema properties, and record, enum and fixed types were resolved without any
 * check. Since {@code SpecificData.getClass(Schema)} runs for every named type on both
 * {@code ReflectDatumWriter.write} and {@code ReflectDatumReader.read}, the widened check rejects
 * Pulsar's own internal Avro types unless they are trusted here.
 *
 * <p>This only ever <em>widens</em> the trusted set: {@link #install()} composes with whatever
 * validator is already installed, so a policy configured by the embedding application, by Avro's
 * {@code org.apache.avro.SERIALIZABLE_CLASSES} / {@code org.apache.avro.SERIALIZABLE_PACKAGES} system
 * properties, or by an earlier call, is preserved rather than replaced.
 *
 * <p><b>Application classes are not covered.</b> POJOs passed to {@code Schema.AVRO(...)} or
 * {@code Schema.JSON(...)} belong to the application, so they must be trusted by the application
 * itself — either through Avro's system properties or by installing a validator via
 * {@link ClassSecurityValidator#setGlobal(ClassSecurityPredicate)}.
 */
@CustomLog
public final class PulsarAvroClassSecurity {

    /**
     * Packages whose types Pulsar serializes with Avro. Matching includes sub-packages, so
     * {@code org.apache.pulsar.broker.transaction.buffer.metadata} also covers its {@code .v2}
     * sub-package, and {@code org.apache.pulsar.common.policies.data} also covers {@code .impl}.
     */
    private static final Set<String> TRUSTED_PACKAGES = ConcurrentHashMap.newKeySet();

    /** Individually trusted classes, for types whose package is too broad to trust wholesale. */
    private static final Set<String> TRUSTED_CLASSES = ConcurrentHashMap.newKeySet();

    /**
     * The predicate this class last installed as the global validator. Retaining the instance rather
     * than a plain "installed" flag keeps {@link #install()} idempotent while still allowing it to
     * reinstall after a test has restored an earlier global value.
     */
    private static ClassSecurityPredicate installedPredicate;

    static {
        addTrustedPackages(
                // Transaction buffer snapshots, written to system topics by
                // TransactionBufferSnapshotBaseSystemTopicClient. Covers both the original
                // (TransactionBufferSnapshot, AbortTxnMetadata) and the .v2 segmented format.
                "org.apache.pulsar.broker.transaction.buffer.metadata",
                // Topic policy events (PulsarEvent, TopicPoliciesEvent, EventType, ActionType)
                // published to the __change_events system topic.
                "org.apache.pulsar.common.events",
                // TopicPolicies and the policy value types it embeds, reached from TopicPoliciesEvent.
                // Sub-package matching also covers org.apache.pulsar.common.policies.data.impl.
                "org.apache.pulsar.common.policies.data",
                // Protobuf runtime types that avro-protobuf resolves when building a schema for a
                // generated message, such as the com.google.protobuf.Any wrapper. The generated message
                // classes themselves belong to the application and are its own to trust. In the shaded
                // client this literal is relocated alongside the protobuf classes it names, so it keeps
                // matching there.
                "com.google.protobuf");
        addTrustedClasses(
                // Replicated by PulsarMetadataEventSynchronizer via Schema.AVRO(MetadataEvent.class).
                "org.apache.pulsar.metadata.api.MetadataEvent",
                "org.apache.pulsar.metadata.api.NotificationType",
                "org.apache.pulsar.metadata.api.extended.CreateOption",
                // Element type of TopicPolicies.subscriptionTypesEnabled. This is a lightproto-generated
                // nested enum, so the name must be the binary one with '$': Avro derives the schema name
                // "...CommandSubscribe.SubType" but resolves it by retrying with '$' separators, and the
                // validator matches on Class.getName(). Trusted per class rather than by package, since
                // org.apache.pulsar.common.api.proto is the whole wire-protocol surface.
                "org.apache.pulsar.common.api.proto.CommandSubscribe$SubType",
                // ReflectData records the *declared* collection type as a "java-class" property on the
                // array or map schema it generates for a field, then resolves it reflectively on both
                // read and write. These are the only three declared in Pulsar's Avro-serialized records
                // (List in the snapshot and policy types, HashSet in MetadataEvent.options and
                // PulsarEvent.replicateTo, Map in TopicPolicies). They are plain containers, not a
                // useful gadget surface. Records outside Pulsar that declare other collection types
                // trust them themselves, the same as any other application class.
                "java.util.List",
                "java.util.Map",
                "java.util.HashSet");
    }

    private PulsarAvroClassSecurity() {
    }

    /**
     * Installs Pulsar's trusted classes into Avro's global class-security validator, composing with
     * the validator that is currently installed. Idempotent, and safe to call from several entry
     * points: it reinstalls only when the global value is no longer the one it last installed.
     */
    public static synchronized void install() {
        ClassSecurityPredicate current = ClassSecurityValidator.getGlobal();
        if (current == installedPredicate) {
            return;
        }
        ClassSecurityPredicate composed =
                ClassSecurityValidator.composite(current, PulsarAvroClassSecurity::isPulsarTrustedAvroClass);
        ClassSecurityValidator.setGlobal(composed);
        installedPredicate = composed;
        log.debug().log("Installed Pulsar's Avro class security validator");
    }

    /**
     * Whether the global validator is still the one {@link #install()} put in place. Test
     * infrastructure uses this to reset validators that a test installed without also undoing Pulsar's
     * own, which a broker shared across test classes still depends on.
     */
    public static synchronized boolean isInstalled() {
        return installedPredicate != null && ClassSecurityValidator.getGlobal() == installedPredicate;
    }

    /**
     * Returns whether the class is one that Pulsar itself serializes with Avro. Package matching
     * requires a package separator at the boundary, so trusting {@code org.apache.pulsar.common}
     * never trusts a class in a package such as {@code org.apache.pulsar.commonEvil}.
     *
     * <p>Array and primitive types are not handled here: {@code ClassSecurityValidator.validate}
     * already unwraps array component types and returns early for primitives.
     */
    public static boolean isPulsarTrustedAvroClass(Class<?> clazz) {
        if (clazz == null) {
            return false;
        }
        // Nested classes report the enclosing package, so Outer$Inner is matched by its package too.
        Package classPackage = clazz.getPackage();
        return isTrusted(clazz.getName(), classPackage == null ? null : classPackage.getName());
    }

    /**
     * String form of {@link #isPulsarTrustedAvroClass(Class)}, so the matching rules can be tested
     * without needing a loadable class in every package under test.
     *
     * @param packageName the class's package, or null for the default package, which is never trusted
     */
    @VisibleForTesting
    static boolean isTrusted(String className, String packageName) {
        if (TRUSTED_CLASSES.contains(className)) {
            return true;
        }
        if (packageName == null) {
            return false;
        }
        for (String trusted : TRUSTED_PACKAGES) {
            // Require a package separator at the boundary so that trusting "org.apache.pulsar.common"
            // does not also trust a package named "org.apache.pulsar.commonEvil".
            if (packageName.equals(trusted) || packageName.startsWith(trusted + ".")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds packages whose classes Pulsar trusts for Avro reflection. Sub-packages are included.
     * Takes effect immediately for an already-installed validator, since the predicate reads the
     * trusted sets on each call.
     */
    public static void addTrustedPackages(String... packageNames) {
        TRUSTED_PACKAGES.addAll(Arrays.asList(packageNames));
    }

    /** Adds individually trusted class names. See {@link #addTrustedPackages(String...)}. */
    public static void addTrustedClasses(String... classNames) {
        TRUSTED_CLASSES.addAll(Arrays.asList(classNames));
    }

    /**
     * Trusts the given classes on top of the currently installed validator and returns that validator,
     * so the caller can restore it once it is done. Intended for tests that need their own fixture
     * classes trusted: unlike {@link #addTrustedClasses(String...)} the added trust is confined to the
     * returned validator, so restoring it also undoes the change.
     *
     * <pre>{@code
     * private ClassSecurityPredicate previousValidator;
     *
     * @BeforeMethod
     * public void trustFixtures() {
     *     previousValidator = PulsarAvroClassSecurity.trustAdditionally(Foo.class, Bar.class);
     * }
     *
     * @AfterMethod(alwaysRun = true)
     * public void restoreValidator() {
     *     PulsarAvroClassSecurity.restoreGlobal(previousValidator);
     * }
     * }</pre>
     *
     * <p>Avro's validator is JVM-global, so a save/restore pair is only reliable when concurrent tests
     * in the same JVM are not racing on it. Prefer trusting a package once for the whole test run over
     * toggling it per test method.
     */
    public static ClassSecurityPredicate trustAdditionally(Class<?>... classes) {
        Set<String> classNames = Arrays.stream(classes).map(Class::getName).collect(Collectors.toSet());
        ClassSecurityPredicate previous = ClassSecurityValidator.getGlobal();
        ClassSecurityValidator.setGlobal(
                ClassSecurityValidator.composite(previous, clazz -> classNames.contains(clazz.getName())));
        return previous;
    }

    /** Restores a validator previously returned by {@link #trustAdditionally(Class[])}. */
    public static void restoreGlobal(ClassSecurityPredicate previous) {
        ClassSecurityValidator.setGlobal(previous);
    }

    @VisibleForTesting
    static Collection<String> getTrustedPackages() {
        return Set.copyOf(TRUSTED_PACKAGES);
    }

    @VisibleForTesting
    static Collection<String> getTrustedClasses() {
        return Set.copyOf(TRUSTED_CLASSES);
    }
}
