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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.net.URI;
import java.util.List;
import java.util.Random;
import org.apache.avro.reflect.Union;
import org.apache.avro.util.ClassSecurityValidator;
import org.apache.avro.util.ClassSecurityValidator.ClassSecurityPredicate;
import org.apache.pulsar.client.api.Schema;
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
    public void testAvroSchemaFailsWithoutDeclaringTrust() {
        Schema<Order> schema = Schema.AVRO(Order.class);

        assertThatThrownBy(() -> schema.encode(Order.sample()))
                .hasRootCauseInstanceOf(SecurityException.class);
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

    private static boolean isTrusted(Class<?> clazz) {
        return ClassSecurityValidator.getGlobal().isTrusted(clazz);
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
