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
import static org.assertj.core.api.Assertions.assertThat;
import java.net.URI;
import java.util.List;
import java.util.Random;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Union;
import org.apache.avro.util.ClassSecurityValidator;
import org.apache.avro.util.ClassSecurityValidator.ClassSecurityPredicate;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.util.SchemaUtil;
import org.apache.pulsar.client.schema.fixtures.Order;
import org.apache.pulsar.client.schema.fixtures.OrderState;
import org.apache.pulsar.client.schema.fixtures.other.Address;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The Gradle test JVM trusts the whole org.apache.pulsar namespace through
 * org.apache.avro.SERIALIZABLE_PACKAGES, which would mask everything these tests are about, so each
 * test drops the global validator to Avro's hardcoded DEFAULT_TRUSTED_CLASSES first — the baseline of
 * a production JVM with no Avro system properties set.
 */
public class AvroTrustedClassesTest {

    private ClassSecurityPredicate previousValidator;

    @BeforeMethod
    public void useProductionBaselineValidator() {
        previousValidator = ClassSecurityValidator.getGlobal();
        ClassSecurityValidator.setGlobal(ClassSecurityValidator.DEFAULT_TRUSTED_CLASSES);
        // Declared trust is process-wide and accumulates, so start each test from the defaults.
        AvroTrustedClasses.resetForTesting();
    }

    @AfterMethod(alwaysRun = true)
    public void restoreValidator() {
        AvroTrustedClasses.resetForTesting();
        ClassSecurityValidator.setGlobal(previousValidator);
    }

    @Test
    public void testSchemaAvroOverAnApplicationClassNeedsNoDeclaration() {
        // Constructing the schema is itself the application naming the class, so Pulsar trusts it and
        // everything the derived schema references. Order pulls in an enum, a record in a *different*
        // package, a List field and a URI field — none of which the user should have to enumerate.
        Schema<Order> schema = Schema.AVRO(Order.class);
        Order order = Order.sample();

        assertThat(schema.decode(schema.encode(order))).usingRecursiveComparison().isEqualTo(order);
        assertThat(isTrusted(Address.class)).isTrue();
        assertThat(isTrusted(OrderState.class)).isTrue();
        assertThat(isTrusted(List.class)).isTrue();
        assertThat(isTrusted(URI.class)).isTrue();
    }

    @Test
    public void testAutoRegistrationDoesNotTrustSchemasArrivingFromTheRegistry() {
        // The trust boundary: a definition built from wire bytes carries no POJO, which is how
        // AutoConsumeSchema passes a registry-fetched schema. Whoever registered that schema chose the
        // class names in it, so they must not widen the allow-list.
        String wireSchema = new String(Schema.AVRO(Order.class).getSchemaInfo().getSchema(), UTF_8);
        AvroTrustedClasses.resetForTesting();

        Schema.AVRO(SchemaDefinition.builder().withJsonDef(wireSchema).build());

        assertThat(isTrusted(Order.class)).isFalse();
        assertThat(isTrusted(Address.class)).isFalse();
    }

    @Test
    public void testAnApplicationClassIsWalkedOnlyOnce() {
        Schema.AVRO(Order.class);
        int afterFirst = AvroTrustedClasses.walkedCountForTesting();

        Schema.AVRO(Order.class);
        Schema.AVRO(Order.class);

        assertThat(AvroTrustedClasses.walkedCountForTesting()).isEqualTo(afterFirst);
        // ...and the trust is still in place after the cached calls.
        assertThat(isTrusted(Address.class)).isTrue();
    }

    @Test
    public void testTrustClassCoversTheWholeObjectGraph() {
        // Order pulls in an enum, a record in a *different* package, a List field and a URI field.
        // Declaring the root class alone has to be enough.
        AvroTrustedClasses.trust(Order.class);

        Schema<Order> schema = Schema.AVRO(Order.class);
        Order order = Order.sample();

        assertThat(schema.decode(schema.encode(order))).usingRecursiveComparison().isEqualTo(order);
    }

    @Test
    public void testTrustClassReachesTypesTheRootPackageDoesNotCover() {
        AvroTrustedClasses.trust(Order.class);

        // Named types in other packages, and the declared field types Avro records as properties.
        assertThat(isTrusted(Address.class)).isTrue();
        assertThat(isTrusted(OrderState.class)).isTrue();
        assertThat(isTrusted(List.class)).isTrue();
        assertThat(isTrusted(URI.class)).isTrue();
        // ...but nothing beyond what the graph actually reaches.
        assertThat(isTrusted(Random.class)).isFalse();
    }

    @Test
    public void testTrustPackagesAloneDoesNotCoverDeclaredCollectionTypes() {
        // Documents why trust(Class) is the primary entry point: the POJO's own package says nothing
        // about java.util.List, which Avro records as a property on the generated array schema.
        AvroTrustedClasses.trustPackages(Order.class.getPackage().getName());

        assertThat(isTrusted(Order.class)).isTrue();
        assertThat(isTrusted(List.class)).isFalse();
    }

    @Test
    public void testTrustClassLoaderIsReachableFromTheFacade() {
        assertThat(isTrusted(Order.class)).isFalse();

        AvroTrustedClasses.trustClassLoader(Order.class.getClassLoader());

        assertThat(isTrusted(Order.class)).isTrue();
    }

    @Test
    public void testTrustExactlyDoesNotFollowReferencedTypes() {
        AvroTrustedClasses.trustExactly(Order.class);

        assertThat(isTrusted(Order.class)).isTrue();
        // Unlike trust(Class), nothing the class references comes along.
        assertThat(isTrusted(Address.class)).isFalse();
        assertThat(isTrusted(OrderState.class)).isFalse();
        assertThat(isTrusted(List.class)).isFalse();
    }

    @Test
    public void testTrustWorksForInterfaces() {
        // Avro derives an empty record for a user interface, so there is nothing to expand, but the
        // interface itself still has to be trusted: a field declaring it is named after it in the schema.
        AvroTrustedClasses.trust(Shape.class);

        assertThat(isTrusted(Shape.class)).isTrue();
    }

    @Test
    public void testTrustWorksForInterfacesWithNoDerivableSchema() {
        // ReflectData cannot describe a bare java.util.List — "Can't find element type of Collection" —
        // so the schema walk fails. Trusting the class itself must still take effect.
        AvroTrustedClasses.trust(List.class);

        assertThat(isTrusted(List.class)).isTrue();
    }

    @Test
    public void testTrustExpandsThroughAUnionOfImplementations() {
        // Where an interface field enumerates its implementations, Avro inlines them as records in the
        // schema, so trusting the holder reaches them. Trusting the interface alone would not.
        AvroTrustedClasses.trust(ShapeHolder.class);

        assertThat(isTrusted(Circle.class)).isTrue();
        assertThat(isTrusted(Square.class)).isTrue();
    }

    @Test
    public void testTrustPredicate() {
        AvroTrustedClasses.trust(clazz -> clazz.getName().equals(Random.class.getName()));

        assertThat(isTrusted(Random.class)).isTrue();
    }

    @Test
    public void testTrustRegistersANestedEnumUnderItsBinaryName() throws Exception {
        // The AUTO_CONSUME-over-a-JSON-enum path resolves the class named in the writer's schema, which
        // Avro spells with dots ("...Test.Colour") and then resolves by retrying with '$'. The validator
        // matches on Class.getName(), so the '$' form is what has to be registered. Reproduces the
        // resolution AutoConsumeSchema.extractFromAvroSchema performs.
        org.apache.avro.Schema enumSchema = SchemaUtil.parseAvroSchema(
                new String(Schema.JSON(Colour.class).getSchemaInfo().getSchema(), UTF_8));
        assertThat(enumSchema.getType()).isEqualTo(org.apache.avro.Schema.Type.ENUM);
        assertThat(enumSchema.getFullName()).contains(".Colour");

        AvroTrustedClasses.trust(Colour.class);

        assertThat(ReflectData.get().getClass(enumSchema)).isSameAs(Colour.class);
    }

    private static boolean isTrusted(Class<?> clazz) {
        return ClassSecurityValidator.getGlobal().isTrusted(clazz);
    }

    /** Nested so that its binary name uses '$' while Avro spells the schema name with dots. */
    public enum Colour {
        RED,
        GREEN
    }

    /** A user interface: Avro derives an empty record for it. */
    public interface Shape {
    }

    public static class Circle implements Shape {
        private int radius = 1;

        public int getRadius() {
            return radius;
        }
    }

    public static class Square implements Shape {
        private int side = 2;

        public int getSide() {
            return side;
        }
    }

    /** Declares the interface's implementations, which is how Avro reflect models polymorphism. */
    public static class ShapeHolder {
        @Union({Circle.class, Square.class})
        private Shape shape = new Circle();

        public Shape getShape() {
            return shape;
        }
    }
}
