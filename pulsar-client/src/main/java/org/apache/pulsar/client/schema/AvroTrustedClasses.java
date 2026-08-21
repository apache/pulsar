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
package org.apache.pulsar.client.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import lombok.CustomLog;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.ClassSecurityValidator;
import org.apache.avro.util.ClassSecurityValidator.ClassSecurityPredicate;
import org.apache.pulsar.client.impl.schema.util.SchemaUtil;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Declares which classes Avro is allowed to reflect over.
 *
 * <p>Avro 1.12.2 refuses to resolve any class that is not explicitly trusted, and that check runs on
 * every reflective class resolution — including the ones {@code Schema.AVRO(...)} performs on each
 * produce and consume.
 *
 * <p><b>Most applications need nothing from this class.</b> Passing a class to {@code Schema.AVRO(...)}
 * is itself the application naming it, so Pulsar trusts that class and every type the schema derived
 * from it references — nested records and enums, including ones in other packages, and the declared
 * collection and {@code @Stringable} types its fields carry. Pulsar Functions and connectors are covered
 * the same way, since they build their schemas through the same call.
 *
 * <p>What is deliberately <em>not</em> covered is a class named only by a schema that arrived over the
 * wire. A schema fetched from the registry names classes chosen by whoever registered it, and resolving
 * those is what the allow-list exists to constrain. {@code Schema.AUTO_CONSUME()} stays clear of this
 * for the usual case: a record schema is decoded generically into a {@code GenericRecord} and no class
 * is resolved at all. Only a topic whose schema is <em>not</em> a record at the top level reaches Avro
 * reflection, and then every named type the schema mentions — record, enum or fixed — is resolved,
 * including through a container: an array or map of records resolves the record class as soon as there
 * is an element to read. An application that consumes such a topic declares the class itself:
 *
 * <pre>{@code
 * // Once, before the first producer or consumer.
 * AvroTrustedClasses.trust(Colour.class);
 * }</pre>
 *
 * <p>{@link #trust(Class[])} follows what the class references, the same way auto-registration does.
 * Prefer it to {@link #trustPackages(String...)}: a package rarely covers a POJO on its own, because
 * Avro also resolves the <em>declared</em> collection type of a field, so a POJO with a {@code List}
 * field needs {@code java.util.List} as well.
 *
 * <p>Declarations are process-wide and accumulate. Code that needs to make a temporary change — a test
 * asserting that some class is <em>not</em> trusted, most often — takes a {@link #snapshot()} first and
 * hands it back to {@link #restore(Snapshot)} afterwards, which puts back exactly what was declared
 * before rather than discarding declarations that belong to something else:
 *
 * <pre>{@code
 * Snapshot before = AvroTrustedClasses.snapshot();
 * try {
 *     AvroTrustedClasses.resetToDefaults();
 *     // ... assert what is and is not trusted ...
 * } finally {
 *     AvroTrustedClasses.restore(before);
 * }
 * }</pre>
 *
 * <p>This class behaves identically in the shaded and unshaded clients. Avro's own API does not:
 * shading relocates {@code org.apache.avro.util.ClassSecurityValidator}, so an application depending
 * on {@code pulsar-client} or {@code pulsar-client-all} cannot name it, and the Avro system property
 * is renamed along with it. For the same reason, the advice in Avro's own {@code SecurityException}
 * message does not apply to the shaded clients.
 *
 * <p>Trust widens the set Avro will accept; it never narrows it. Whatever validator is already
 * installed — Avro's own defaults, a policy from the {@code SERIALIZABLE_CLASSES} /
 * {@code SERIALIZABLE_PACKAGES} system properties, or one the application installed itself — keeps
 * working, because this class composes with it rather than replacing it. Composition happens once;
 * every later declaration is an append, so it takes effect immediately and costs nothing on the
 * serialization path.
 *
 * <p>Declaring trust after a rejection still works for subsequent operations; the one that already
 * failed has failed. Note that Avro caches a class once it has resolved it, so
 * {@link #untrustClassLoader(ClassLoader)} stops further resolutions rather than undoing past ones.
 *
 * <p>Only classes reached through Avro schemas are affected. {@code Schema.JSON(...)} reads and
 * writes through Jackson and needs nothing declared here.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@CustomLog
public final class AvroTrustedClasses {

    /**
     * Everything declared through this class, as one replaceable unit. Swapping the reference is how
     * {@link #restore(Snapshot)} puts back a previous set of declarations atomically.
     */
    private static volatile Snapshot current = Snapshot.withDefaults();

    /**
     * Whether the Avro on the class path has a trusted-class validator to install into at all. It was
     * introduced in Avro 1.12.2; an application using {@code pulsar-client-original} can pin an older
     * Avro, and on those versions there is nothing to enforce and so nothing to declare. Everything
     * this class does then becomes a no-op rather than a {@link NoClassDefFoundError} raised out of
     * {@code Schema.AVRO(...)}.
     */
    private static final boolean VALIDATOR_AVAILABLE = isValidatorAvailable();

    private static boolean isValidatorAvailable() {
        try {
            Class.forName("org.apache.avro.util.ClassSecurityValidator", false,
                    AvroTrustedClasses.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            log.debug().log("Avro has no ClassSecurityValidator; trusted-class declarations are a no-op");
            return false;
        }
    }

    private AvroTrustedClasses() {
    }

    /**
     * The set of declarations held by {@link AvroTrustedClasses} at one point in time.
     *
     * <p>Opaque: obtain one from {@link AvroTrustedClasses#snapshot()} and hand it back to
     * {@link AvroTrustedClasses#restore(Snapshot)}. Taking a snapshot copies the declarations, so it is
     * unaffected by anything declared afterwards and can be restored more than once.
     */
    public static final class Snapshot {

        /** Trusted package prefixes; sub-packages are included. */
        private final Set<String> trustedPackages = ConcurrentHashMap.newKeySet();

        /** Individually trusted binary class names. */
        private final Set<String> trustedClasses = ConcurrentHashMap.newKeySet();

        /**
         * Class loaders whose classes are trusted wholesale. Held weakly so that declaring trust does
         * not keep a loader, or the classes it defined, from being collected.
         */
        private final Set<ClassLoader> trustedClassLoaders =
                Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

        /** Additional rules, for policies the name- and loader-based methods cannot express. */
        private final Set<Predicate<Class<?>>> trustedPredicates = ConcurrentHashMap.newKeySet();

        /**
         * Application classes whose derived schema has already been walked, so a class used for many
         * producers or consumers is traversed once rather than on every schema construction. Holds
         * names rather than Class objects, so it pins neither a class nor its loader.
         */
        private final Set<WalkKey> walked = ConcurrentHashMap.newKeySet();

        /**
         * Identifies one traversal, by the schema that was walked rather than by the class alone. The
         * same class can derive different schemas — JSR-310 conversions replace a named type with a
         * logical primitive, and two class loaders can define different versions of the same class name
         * — and each of those names a different set of types. Keying on the class alone could cache the
         * smaller set and then refuse a type the larger one needs, which fails closed at serialization
         * time. Holds names and schema text only, so it pins neither a class nor a loader.
         */
        private record WalkKey(String className, String schemaJson) {
        }

        private Snapshot() {
        }

        /** The built-in baseline: what a JVM starts out with before anything is declared. */
        private static Snapshot withDefaults() {
            Snapshot snapshot = new Snapshot();
            // Protobuf runtime types that avro-protobuf resolves when building a schema for a generated
            // message, such as the com.google.protobuf.Any wrapper. Needed by Schema.PROTOBUF for any
            // application, so it belongs to the client's own baseline rather than to a caller. In the
            // shaded client this literal is relocated alongside the classes it names, so it still
            // matches.
            snapshot.trustedPackages.add("com.google.protobuf");
            // Collection types that ReflectData records as a "java-class" property on the array or map
            // schema it generates for a field, and then resolves reflectively. Trusting a class expands
            // through the derived schema and picks these up on its own, but declaring a package or a
            // bare class name does not, which made "trust my model package" quietly insufficient for
            // any POJO with a List field. They are plain containers with nothing exploitable in
            // construction, and Avro's own build trusts the same set, so there is no value in making
            // callers rediscover them.
            snapshot.trustedClasses.addAll(Arrays.asList(
                    "java.util.Collection",
                    "java.util.List",
                    "java.util.ArrayList",
                    "java.util.Set",
                    "java.util.HashSet",
                    "java.util.LinkedHashSet",
                    "java.util.TreeSet",
                    "java.util.Map",
                    "java.util.HashMap",
                    "java.util.LinkedHashMap",
                    "java.util.TreeMap",
                    "java.util.concurrent.ConcurrentHashMap"));
            return snapshot;
        }

        /**
         * An independent copy. Both {@code snapshot()} and {@code restore()} copy, so a snapshot never
         * shares mutable state with the declarations that are live.
         */
        private Snapshot copy() {
            Snapshot copy = new Snapshot();
            copy.trustedPackages.addAll(trustedPackages);
            copy.trustedClasses.addAll(trustedClasses);
            // Copies the references, not the loaders: the copy holds them weakly as well.
            synchronized (trustedClassLoaders) {
                copy.trustedClassLoaders.addAll(trustedClassLoaders);
            }
            copy.trustedPredicates.addAll(trustedPredicates);
            copy.walked.addAll(walked);
            return copy;
        }

        private void addClassName(String className) {
            if (className != null && !className.isEmpty()) {
                trustedClasses.add(className);
            }
        }

        private void addClassNames(Collection<String> classNames) {
            for (String className : classNames) {
                addClassName(className);
            }
        }

        private void addPackageNames(Collection<String> packageNames) {
            for (String packageName : packageNames) {
                if (packageName != null && !packageName.isEmpty()) {
                    trustedPackages.add(packageName);
                }
            }
        }

        private void addClassLoader(ClassLoader classLoader) {
            trustedClassLoaders.add(classLoader);
        }

        private void removeClassLoader(ClassLoader classLoader) {
            trustedClassLoaders.remove(classLoader);
        }

        private void addPredicate(Predicate<Class<?>> predicate) {
            trustedPredicates.add(predicate);
        }

        /** Records a traversal, returning false when this schema has already been walked. */
        private boolean markWalked(String className, String schemaJson) {
            return walked.add(new WalkKey(className, schemaJson));
        }

        private int walkedCount() {
            return walked.size();
        }

        /** Whether the class is covered by anything declared here. */
        private boolean isTrusted(Class<?> clazz) {
            if (clazz == null) {
                return false;
            }
            // Nested classes report the enclosing package, so Outer$Inner is matched by its package too.
            Package classPackage = clazz.getPackage();
            if (isTrustedName(clazz.getName(), classPackage == null ? null : classPackage.getName())) {
                return true;
            }
            if (isLoadedByTrustedClassLoader(clazz)) {
                return true;
            }
            for (Predicate<Class<?>> predicate : trustedPredicates) {
                if (predicate.test(clazz)) {
                    return true;
                }
            }
            return false;
        }

        private boolean isTrustedName(String className, String packageName) {
            if (trustedClasses.contains(className)) {
                return true;
            }
            if (packageName == null) {
                return false;
            }
            for (String trusted : trustedPackages) {
                // Require a package separator at the boundary so that trusting "com.example.model" does
                // not also trust a package named "com.example.modelExtra".
                if (packageName.equals(trusted) || packageName.startsWith(trusted + ".")) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Whether the class was defined by a trusted class loader, or by a descendant of one. Walking up
         * the parent chain only ever reaches loaders that were explicitly declared, so a class from the
         * application or system class loader is not trusted unless that loader itself was declared.
         */
        private boolean isLoadedByTrustedClassLoader(Class<?> clazz) {
            if (trustedClassLoaders.isEmpty()) {
                return false;
            }
            for (ClassLoader loader = clazz.getClassLoader(); loader != null; loader = loader.getParent()) {
                if (trustedClassLoaders.contains(loader)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Copies everything declared so far, for handing back to {@link #restore(Snapshot)} later. The copy
     * is unaffected by anything declared afterwards.
     */
    public static Snapshot snapshot() {
        return current.copy();
    }

    /**
     * Replaces the declarations with those of a snapshot, discarding anything declared since it was
     * taken and restoring anything that has been dropped. The snapshot is copied rather than adopted,
     * so it stays valid and can be restored again.
     *
     * <p>Pair this with {@link #snapshot()} around a temporary change. It is what a test should use
     * instead of {@link #resetToDefaults()} alone, since other components — a broker running in the
     * same JVM, for instance — have declarations of their own that must survive the test.
     *
     * <p>Restoring only affects declarations made through this class. Avro's global validator is left
     * as it is; code that swapped that out is responsible for putting it back.
     */
    public static void restore(Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        current = snapshot.copy();
        install();
    }

    /**
     * Drops every declaration made through this class and returns to the built-in defaults — the
     * protobuf runtime package and the collection types Avro records as {@code java-class} properties.
     * Trust that did not come from this class is untouched, including Avro's own
     * {@code SERIALIZABLE_CLASSES} and {@code SERIALIZABLE_PACKAGES} properties and any validator
     * installed directly through Avro.
     *
     * <p>Chiefly for tests, and best paired with {@link #snapshot()} and {@link #restore(Snapshot)} so
     * that declarations belonging to other components come back afterwards. Declared trust is
     * process-wide and accumulates, and building a schema declares its class on its own, so a test
     * asserting that something is <em>not</em> trusted only means anything if it starts from a known
     * state — otherwise an unrelated earlier test can leave the class trusted and the assertion passes
     * vacuously.
     *
     * <p>Take care outside tests. Declarations are shared by the whole JVM, so this drops what other
     * components declared as well, and schemas built earlier do not re-declare anything. Avro caches
     * the classes it has already resolved, so the effect usually shows up later, as a refusal of some
     * type that had not been resolved yet, rather than immediately.
     */
    public static void resetToDefaults() {
        current = Snapshot.withDefaults();
        install();
    }

    /** How many derived schemas have been walked. Only for tests. */
    @VisibleForTesting
    static int walkedCountForTesting() {
        return current.walkedCount();
    }

    /**
     * Name-based form of the trust check, so the matching rules can be tested without needing a
     * loadable class in every package under test.
     *
     * @param packageName the class's package, or null for the default package, which is never trusted
     */
    @VisibleForTesting
    static boolean isTrustedName(String className, String packageName) {
        return current.isTrustedName(className, packageName);
    }

    /**
     * Trusts each given class along with everything Avro resolves when serializing it: the nested
     * records, enums and fixed types reachable from its fields, whatever package they live in, and the
     * declared collection and {@code @Stringable} types its fields carry.
     *
     * <p>Prefer this to the name-based methods. Call it once per POJO you use with
     * {@code Schema.AVRO(...)}, before creating the producer or consumer.
     *
     * <p>If a class has no schema Avro can derive — a bare {@code java.util.List}, say, which has no
     * element type to describe — only the class itself is trusted; there is nothing to expand. Use
     * {@link #trustExactly(Class[])} when that is what you meant.
     */
    public static void trust(Class<?>... classes) {
        Snapshot snapshot = current;
        for (Class<?> clazz : classes) {
            if (clazz == null) {
                continue;
            }
            snapshot.addClassName(clazz.getName());
            snapshot.addClassNames(typesReachableFrom(clazz));
        }
        install();
    }

    /**
     * Trusts a class the application supplied to a Pulsar schema, together with every type the schema
     * Pulsar derived from it references. Called by Pulsar when it builds an Avro schema from an
     * application class, so applications normally do not have to declare anything themselves.
     *
     * <p>The trust boundary this relies on is that {@code pojo} came from application code. It must
     * therefore never be called with a class named by a schema that arrived over the wire — a schema
     * fetched from the registry names classes chosen by whoever registered it, and resolving those is
     * exactly what the allow-list exists to constrain.
     *
     * <p>Takes the derived {@link SchemaInfo} rather than re-deriving from the class, so the types
     * trusted are exactly the ones the schema in use actually names. Deriving a second time with a
     * differently configured {@code ReflectData} would not produce the same set.
     *
     * <p>Use {@link #trustExactly(Class[])} instead where there is no derived schema to expand from.
     *
     * @param pojo the application class, or null to do nothing
     * @param schemaInfo the schema Pulsar derived from it
     */
    @InterfaceAudience.Private
    public static void trustApplicationSchema(Class<?> pojo, SchemaInfo schemaInfo) {
        if (pojo == null) {
            return;
        }
        Snapshot snapshot = current;
        snapshot.addClassName(pojo.getName());
        if (schemaInfo == null || schemaInfo.getSchema() == null || schemaInfo.getSchema().length == 0) {
            install();
            return;
        }
        String schemaJson = new String(schemaInfo.getSchema(), UTF_8);
        if (snapshot.markWalked(pojo.getName(), schemaJson)) {
            try {
                Set<String> names = new HashSet<>();
                // Resolve names against the loader that defined the POJO: that is where the types it
                // references live, and it is the loader Avro itself uses for them.
                collectTypeNames(SchemaUtil.parseAvroSchema(schemaJson),
                        Collections.newSetFromMap(new IdentityHashMap<>()), names, pojo.getClassLoader());
                snapshot.addClassNames(names);
            } catch (RuntimeException e) {
                // A schema Pulsar just derived should always parse; if it somehow does not, the class
                // itself is still trusted and an explicit trust(...) call remains available.
                log.debug().attr("class", pojo.getName()).exception(e)
                        .log("Could not expand trust from the derived schema");
            }
        }
        // Deliberately outside the cache: install() is what re-composes Pulsar's predicate if something
        // else has replaced the global validator since, and it is a reference comparison once installed.
        install();
    }

    /**
     * Trusts every class in the given packages, and in their sub-packages. Matching requires a package
     * separator at the boundary, so trusting {@code com.example.model} does not trust a package named
     * {@code com.example.modelExtra}.
     *
     * <p>Remember that a package rarely covers a POJO on its own — see the note on
     * {@link #trust(Class[])} about declared collection types.
     */
    public static void trustPackages(String... packageNames) {
        current.addPackageNames(Arrays.asList(packageNames));
        install();
    }

    /**
     * Trusts exactly the given classes, without following what they reference. Use this when you want
     * to say precisely which classes are trusted; {@link #trust(Class[])} is usually the more practical
     * choice, since it also covers the types Avro reaches from them.
     *
     * <p>Works for interfaces too, which is the one case where {@link #trust(Class[])} has nothing to
     * expand: an interface either has no schema of its own (Avro generates an empty record) or none at
     * all (a bare {@code java.util.List} cannot be described without an element type). Note that
     * trusting an interface does not trust its implementations — Avro names the concrete class in the
     * schema, so those have to be trusted in their own right, which {@link #trust(Class[])} does
     * automatically for a class whose fields declare them with {@code @Union}.
     */
    public static void trustExactly(Class<?>... classes) {
        if (classes != null) {
            Snapshot snapshot = current;
            for (Class<?> clazz : classes) {
                if (clazz != null) {
                    snapshot.addClassName(clazz.getName());
                }
            }
        }
        // Installs even when nothing was named, so that the built-in defaults - the protobuf runtime
        // types and the collection types Avro records as java-class properties - take effect in a JVM
        // whose schemas are all built from a schema document rather than from a class.
        install();
    }

    /**
     * Trusts individually named classes, without following what they reference. Use this for classes
     * you cannot reference at compile time. Nested classes must be named with the binary form, using
     * {@code $} rather than a dot: {@code com.example.Outer$Inner}.
     */
    public static void trustClasses(String... classNames) {
        current.addClassNames(Arrays.asList(classNames));
        install();
    }

    /**
     * Trusts every class defined by the given class loader, or by a descendant of it. Intended for an
     * application that loads plugin or tenant code into a class loader of its own and wants the classes
     * it defines trusted wholesale.
     *
     * <p>Pulsar itself does not use this: Functions and connectors build their schemas through
     * {@code Schema.AVRO(...)} like any other application, so their classes are declared by name when
     * the schema is built. Prefer that where you can — trusting a loader also trusts every third-party
     * class packaged alongside the code you meant to trust.
     */
    public static void trustClassLoader(ClassLoader classLoader) {
        if (classLoader == null) {
            return;
        }
        current.addClassLoader(classLoader);
        install();
    }

    /**
     * Stops trusting a class loader declared with {@link #trustClassLoader(ClassLoader)}. Classes Avro
     * has already resolved stay resolvable — Avro caches them — so this governs future resolutions.
     */
    public static void untrustClassLoader(ClassLoader classLoader) {
        if (classLoader != null) {
            current.removeClassLoader(classLoader);
        }
    }

    /**
     * Trusts any class the given predicate accepts. An escape hatch for policies the other methods
     * cannot express; the predicate is consulted on the serialization path, so keep it cheap.
     */
    public static void trust(Predicate<Class<?>> predicate) {
        if (predicate != null) {
            current.addPredicate(predicate);
            install();
        }
    }

    /**
     * Installs this class's trust into Avro's global validator, when there is one to install into.
     * Idempotent: after the first call the normal path is a single reference comparison, and declaring
     * more trust never composes again.
     */
    private static void install() {
        if (VALIDATOR_AVAILABLE) {
            Validator.install();
        }
    }

    /**
     * Whether the global validator is still the one this class installed. Always false when the Avro on
     * the class path has no validator.
     */
    public static boolean isInstalled() {
        return VALIDATOR_AVAILABLE && Validator.isInstalled();
    }

    /** Whether the class is covered by anything declared through this class. */
    private static boolean isTrustedClass(Class<?> clazz) {
        return current.isTrusted(clazz);
    }

    /**
     * Holds every reference to Avro's validator types, so they are resolved only when this nested class
     * is first used. Keeping them out of {@link AvroTrustedClasses} itself is what lets that class load,
     * and its methods run harmlessly, against an Avro that predates the validator.
     */
    private static final class Validator {

        /**
         * Pulsar's contribution to the global validator, as a single stable instance. It reads whatever
         * declarations are current on every call, so declaring more trust — or restoring a snapshot —
         * takes effect immediately without composing into the global validator again; composition
         * happens only in {@link #install()}.
         *
         * <p>Holding one instance keeps repeated installs cheap, but it does not make duplicates
         * impossible: if something composes its own predicate on top of ours, the identity check no
         * longer matches and the next install composes a second reference to this instance. That is
         * harmless — {@code composite} is a short-circuiting OR — just not free.
         */
        private static final ClassSecurityPredicate TRUSTED_PREDICATE = AvroTrustedClasses::isTrustedClass;

        /** The predicate last installed, so {@link #install()} can tell whether it is still active. */
        private static ClassSecurityPredicate installedPredicate;

        private Validator() {
        }

        static synchronized void install() {
            ClassSecurityPredicate current = ClassSecurityValidator.getGlobal();
            if (current == installedPredicate) {
                return;
            }
            ClassSecurityPredicate composed = ClassSecurityValidator.composite(current, TRUSTED_PREDICATE);
            ClassSecurityValidator.setGlobal(composed);
            installedPredicate = composed;
            log.debug().log("Installed Pulsar's Avro trusted-class validator");
        }

        static synchronized boolean isInstalled() {
            return installedPredicate != null && ClassSecurityValidator.getGlobal() == installedPredicate;
        }
    }

    /**
     * Names every type Avro will resolve when serializing the class, taken from the schema it
     * generates for it. Schema generation uses plain reflection and never consults the validator, so
     * this is safe to do before the class is trusted.
     */
    private static Set<String> typesReachableFrom(Class<?> clazz) {
        Schema schema;
        try {
            schema = ReflectData.get().getSchema(clazz);
        } catch (RuntimeException e) {
            // Not every class has a derivable schema (a bare interface, for example). The class itself
            // has already been trusted, which is the useful part.
            log.debug().attr("class", clazz.getName()).exception(e)
                    .log("Could not derive an Avro schema to expand trust from");
            return Collections.emptySet();
        }
        Set<String> names = new HashSet<>();
        collectTypeNames(schema, Collections.newSetFromMap(new IdentityHashMap<>()), names,
                clazz.getClassLoader());
        return names;
    }

    private static void collectTypeNames(Schema schema, Set<Schema> visited, Set<String> names,
                                         ClassLoader loader) {
        if (schema == null || !visited.add(schema)) {
            return;
        }
        // Collection and @Stringable types are recorded as properties naming the declared class.
        addIfPresent(schema.getProp(SpecificData.CLASS_PROP), names);
        addIfPresent(schema.getProp(SpecificData.KEY_CLASS_PROP), names);
        addIfPresent(schema.getProp(SpecificData.ELEMENT_PROP), names);

        switch (schema.getType()) {
            case RECORD:
                addNamedType(schema, names, loader);
                for (Schema.Field field : schema.getFields()) {
                    collectTypeNames(field.schema(), visited, names, loader);
                }
                break;
            case ENUM:
            case FIXED:
                addNamedType(schema, names, loader);
                break;
            case ARRAY:
                collectTypeNames(schema.getElementType(), visited, names, loader);
                break;
            case MAP:
                collectTypeNames(schema.getValueType(), visited, names, loader);
                break;
            case UNION:
                for (Schema member : schema.getTypes()) {
                    collectTypeNames(member, visited, names, loader);
                }
                break;
            default:
                break;
        }
    }

    /**
     * Records a named type under the binary name of the class it resolves to. Avro spells a nested
     * class {@code a.b.Outer.Inner} in the schema but resolves it by retrying with {@code $}
     * separators, and the validator matches on {@code Class.getName()}, so the {@code $} form is the
     * one that has to be trusted.
     */
    private static void addNamedType(Schema schema, Set<String> names, ClassLoader loader) {
        String fullName = schema.getFullName();
        if (fullName == null) {
            return;
        }
        String resolved = resolveBinaryName(fullName, loader);
        names.add(resolved != null ? resolved : fullName);
    }

    private static String resolveBinaryName(String fullName, ClassLoader preferredLoader) {
        ClassLoader loader = preferredLoader;
        if (loader == null) {
            loader = Thread.currentThread().getContextClassLoader();
        }
        if (loader == null) {
            loader = AvroTrustedClasses.class.getClassLoader();
        }
        // Mirrors SpecificData.getClass: try the name as written, then progressively replace trailing
        // dots with '$' to find a nested class. Uses plain reflection with initialize=false, so it
        // neither runs static initializers nor consults Avro's validator.
        if (classExists(fullName, loader)) {
            return fullName;
        }
        StringBuilder candidate = new StringBuilder(fullName);
        int lastDot = fullName.lastIndexOf('.');
        while (lastDot != -1) {
            candidate.setCharAt(lastDot, '$');
            if (classExists(candidate.toString(), loader)) {
                return candidate.toString();
            }
            lastDot = fullName.lastIndexOf('.', lastDot - 1);
        }
        return null;
    }

    private static boolean classExists(String name, ClassLoader loader) {
        try {
            Class.forName(name, false, loader);
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    private static void addIfPresent(String className, Set<String> names) {
        if (className != null && !className.isEmpty()) {
            names.add(className);
        }
    }
}
