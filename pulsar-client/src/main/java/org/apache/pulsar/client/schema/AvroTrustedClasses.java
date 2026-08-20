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
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
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
 * those is what the allow-list exists to constrain. The cases that reach Avro reflection that way are
 * narrow — chiefly {@code Schema.AUTO_CONSUME()} against a topic whose schema is a bare enum — and an
 * application that hits one declares the class itself:
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

    /** Trusted package prefixes; sub-packages are included. */
    private static final Set<String> TRUSTED_PACKAGES = ConcurrentHashMap.newKeySet();

    /** Individually trusted binary class names. */
    private static final Set<String> TRUSTED_CLASSES = ConcurrentHashMap.newKeySet();

    /**
     * Class loaders whose classes are trusted wholesale. Held weakly so that declaring trust does not
     * keep a loader, or the classes it defined, from being collected.
     */
    private static final Set<ClassLoader> TRUSTED_CLASS_LOADERS =
            Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    /** Additional rules, for policies the name- and loader-based methods cannot express. */
    private static final Set<Predicate<Class<?>>> TRUSTED_PREDICATES = ConcurrentHashMap.newKeySet();

    /**
     * Application classes whose derived schema has already been walked, so a class used for many
     * producers or consumers is traversed once rather than on every schema construction. Holds names
     * rather than Class objects, so it pins neither a class nor its loader.
     */
    private static final Set<WalkKey> WALKED = ConcurrentHashMap.newKeySet();

    /**
     * Identifies one traversal. The class alone is not enough: the same class derives a different schema
     * depending on whether JSR-310 conversions are enabled, and a registered conversion replaces a named
     * type with a logical primitive, so one setting yields strictly fewer names than the other. Keying on
     * the class alone could cache the smaller set and under-trust the larger one, which fails closed at
     * serialization time. Holds the class NAME rather than the Class, so it pins neither the class nor
     * its loader.
     */
    private record WalkKey(String className, boolean jsr310ConversionEnabled) {
    }

    /**
     * This class's contribution to the global validator, as a single stable instance. It reads the
     * sets above on every call, so declaring more trust takes effect immediately without composing
     * into the global validator again — composition happens only in {@link #install()}. Holding one
     * instance also means a reinstall cannot accumulate several copies of this predicate in the chain.
     */
    private static final ClassSecurityPredicate TRUSTED_PREDICATE = AvroTrustedClasses::isTrustedClass;

    /** The predicate this class last installed, so {@link #install()} can tell if it is still active. */
    private static ClassSecurityPredicate installedPredicate;

    static {
        seedDefaults();
    }

    private static void seedDefaults() {
        // Protobuf runtime types that avro-protobuf resolves when building a schema for a generated
        // message, such as the com.google.protobuf.Any wrapper. Needed by Schema.PROTOBUF for any
        // application, so it belongs to the client's own baseline rather than to a caller. In the
        // shaded client this literal is relocated alongside the classes it names, so it still matches.
        TRUSTED_PACKAGES.add("com.google.protobuf");
    }

    /**
     * Drops everything declared so far and returns to the built-in defaults. Only for tests: trust is
     * process-wide and accumulates, so a test that asserts something is <em>not</em> trusted has to
     * start from a known state. Does not touch Avro's global validator.
     */
    /** How many application classes have had their derived schema walked. Only for tests. */
    @VisibleForTesting
    static int walkedCountForTesting() {
        return WALKED.size();
    }

    @VisibleForTesting
    static synchronized void resetForTesting() {
        TRUSTED_PACKAGES.clear();
        TRUSTED_CLASSES.clear();
        TRUSTED_CLASS_LOADERS.clear();
        TRUSTED_PREDICATES.clear();
        WALKED.clear();
        installedPredicate = null;
        seedDefaults();
    }

    private AvroTrustedClasses() {
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
        for (Class<?> clazz : classes) {
            if (clazz == null) {
                continue;
            }
            TRUSTED_CLASSES.add(clazz.getName());
            TRUSTED_CLASSES.addAll(typesReachableFrom(clazz));
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
     * @param pojo the application class, or null to do nothing
     * @param schemaInfo the schema Pulsar derived from it
     */
    @InterfaceAudience.Private
    public static void trustApplicationSchema(Class<?> pojo, SchemaInfo schemaInfo) {
        if (pojo == null) {
            return;
        }
        TRUSTED_CLASSES.add(pojo.getName());
        if (schemaInfo != null && schemaInfo.getSchema() != null && schemaInfo.getSchema().length > 0
                && WALKED.add(new WalkKey(pojo.getName(), SchemaUtil.getJsr310ConversionEnabled(schemaInfo)))) {
            try {
                Set<String> names = new HashSet<>();
                collectTypeNames(SchemaUtil.parseAvroSchema(new String(schemaInfo.getSchema(), UTF_8)),
                        Collections.newSetFromMap(new IdentityHashMap<>()), names);
                TRUSTED_CLASSES.addAll(names);
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
        TRUSTED_PACKAGES.addAll(Arrays.asList(packageNames));
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
        for (Class<?> clazz : classes) {
            if (clazz != null) {
                TRUSTED_CLASSES.add(clazz.getName());
            }
        }
        install();
    }

    /**
     * Trusts individually named classes, without following what they reference. Use this for classes
     * you cannot reference at compile time. Nested classes must be named with the binary form, using
     * {@code $} rather than a dot: {@code com.example.Outer$Inner}.
     */
    public static void trustClasses(String... classNames) {
        TRUSTED_CLASSES.addAll(Arrays.asList(classNames));
        install();
    }

    /**
     * Trusts every class defined by the given class loader, or by a descendant of it. Intended for
     * code loaded and run on the deployment's behalf in a class loader created for that purpose —
     * Pulsar Functions and connectors do this for the code they run, and an application that loads
     * plugin or tenant code can do the same.
     */
    public static void trustClassLoader(ClassLoader classLoader) {
        if (classLoader == null) {
            return;
        }
        TRUSTED_CLASS_LOADERS.add(classLoader);
        install();
    }

    /**
     * Stops trusting a class loader declared with {@link #trustClassLoader(ClassLoader)}. Classes Avro
     * has already resolved stay resolvable — Avro caches them — so this governs future resolutions.
     */
    public static void untrustClassLoader(ClassLoader classLoader) {
        if (classLoader != null) {
            TRUSTED_CLASS_LOADERS.remove(classLoader);
        }
    }

    /**
     * Trusts any class the given predicate accepts. An escape hatch for policies the other methods
     * cannot express; the predicate is consulted on the serialization path, so keep it cheap.
     */
    public static void trust(Predicate<Class<?>> predicate) {
        if (predicate != null) {
            TRUSTED_PREDICATES.add(predicate);
            install();
        }
    }

    /**
     * Installs this class's trust into Avro's global validator, composing with the validator that is
     * currently installed. Idempotent: after the first call the normal path is a single reference
     * comparison, and declaring more trust never composes again.
     */
    private static synchronized void install() {
        ClassSecurityPredicate current = ClassSecurityValidator.getGlobal();
        if (current == installedPredicate) {
            return;
        }
        ClassSecurityPredicate composed = ClassSecurityValidator.composite(current, TRUSTED_PREDICATE);
        ClassSecurityValidator.setGlobal(composed);
        installedPredicate = composed;
        log.debug().log("Installed Pulsar's Avro trusted-class validator");
    }

    /**
     * Whether the global validator is still the one this class installed. Test infrastructure uses
     * this to reset a validator a test installed without also undoing Pulsar's own.
     */
    public static synchronized boolean isInstalled() {
        return installedPredicate != null && ClassSecurityValidator.getGlobal() == installedPredicate;
    }

    /** Whether the class is covered by anything declared through this class. */
    private static boolean isTrustedClass(Class<?> clazz) {
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
        for (Predicate<Class<?>> predicate : TRUSTED_PREDICATES) {
            if (predicate.test(clazz)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Name-based form of the check, so the matching rules can be tested without needing a loadable
     * class in every package under test.
     *
     * @param packageName the class's package, or null for the default package, which is never trusted
     */
    @VisibleForTesting
    static boolean isTrustedName(String className, String packageName) {
        if (TRUSTED_CLASSES.contains(className)) {
            return true;
        }
        if (packageName == null) {
            return false;
        }
        for (String trusted : TRUSTED_PACKAGES) {
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
    private static boolean isLoadedByTrustedClassLoader(Class<?> clazz) {
        if (TRUSTED_CLASS_LOADERS.isEmpty()) {
            return false;
        }
        for (ClassLoader loader = clazz.getClassLoader(); loader != null; loader = loader.getParent()) {
            if (TRUSTED_CLASS_LOADERS.contains(loader)) {
                return true;
            }
        }
        return false;
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
        collectTypeNames(schema, Collections.newSetFromMap(new IdentityHashMap<>()), names);
        return names;
    }

    private static void collectTypeNames(Schema schema, Set<Schema> visited, Set<String> names) {
        if (schema == null || !visited.add(schema)) {
            return;
        }
        // Collection and @Stringable types are recorded as properties naming the declared class.
        addIfPresent(schema.getProp(SpecificData.CLASS_PROP), names);
        addIfPresent(schema.getProp(SpecificData.KEY_CLASS_PROP), names);
        addIfPresent(schema.getProp(SpecificData.ELEMENT_PROP), names);

        switch (schema.getType()) {
            case RECORD:
                addNamedType(schema, names);
                for (Schema.Field field : schema.getFields()) {
                    collectTypeNames(field.schema(), visited, names);
                }
                break;
            case ENUM:
            case FIXED:
                addNamedType(schema, names);
                break;
            case ARRAY:
                collectTypeNames(schema.getElementType(), visited, names);
                break;
            case MAP:
                collectTypeNames(schema.getValueType(), visited, names);
                break;
            case UNION:
                for (Schema member : schema.getTypes()) {
                    collectTypeNames(member, visited, names);
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
    private static void addNamedType(Schema schema, Set<String> names) {
        String fullName = schema.getFullName();
        if (fullName == null) {
            return;
        }
        String resolved = resolveBinaryName(fullName);
        names.add(resolved != null ? resolved : fullName);
    }

    private static String resolveBinaryName(String fullName) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
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
