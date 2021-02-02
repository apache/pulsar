/**
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
package org.apache.pulsar.tests.quarantine;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.testng.annotations.Test;
import org.testng.internal.annotations.TestAnnotation;

public class QuarantinedTestNGAnnotationTransformerTest {

    @Quarantined
    static class ClassWithQuarantined {
        public void testMethod() {

        }
    }

    static class ClassWithQuarantinedConstructor {
        @Quarantined
        public ClassWithQuarantinedConstructor() {

        }

        public void testMethod() {

        }
    }

    static class ClassWithASingleQuarantinedMethod {
        public ClassWithASingleQuarantinedMethod() {

        }

        public void testMethod() {

        }

        @Quarantined
        public void testMethodQuarantined() {

        }
    }

    @Test
    public void shouldNotRunTestMethodsOnQuarantinedClass() throws NoSuchMethodException {
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(false, false);

        assertFalse(getTestAnnotation(quarantinedTransformer, ClassWithQuarantined.class,
                "testMethod").getEnabled());
    }

    @Test
    public void shouldRunOnlyQuarantinedTestMethodsOnQuarantinedClass() throws NoSuchMethodException {
        // given runOnlyQuarantinedTests=true
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(false, true);

        assertTrue(getTestAnnotation(quarantinedTransformer, ClassWithQuarantined.class,
                "testMethod").getEnabled());
    }

    @Test
    public void shouldRunAlsoQuarantinedTestMethodsOnQuarantinedClass() throws NoSuchMethodException {
        // given runQuarantinedTests=true
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(true, false);

        assertTrue(getTestAnnotation(quarantinedTransformer, ClassWithQuarantined.class,
                "testMethod").getEnabled());
    }


    @Test
    public void shouldNotRunQuarantinedTestMethods() throws NoSuchMethodException {
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(false, false);
        assertTrue(getTestAnnotation(quarantinedTransformer, ClassWithASingleQuarantinedMethod.class,
                "testMethod").getEnabled());
        assertFalse(getTestAnnotation(quarantinedTransformer, ClassWithASingleQuarantinedMethod.class,
                "testMethodQuarantined").getEnabled());
    }

    @Test
    public void shouldRunOnlyQuarantinedTestMethods() throws NoSuchMethodException {
        // given runOnlyQuarantinedTests=true
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(false, true);
        assertFalse(getTestAnnotation(quarantinedTransformer, ClassWithASingleQuarantinedMethod.class,
                "testMethod").getEnabled());
        assertTrue(getTestAnnotation(quarantinedTransformer, ClassWithASingleQuarantinedMethod.class,
                "testMethodQuarantined").getEnabled());
    }

    @Test
    public void shouldRunAlsoQuarantinedTestMethods() throws NoSuchMethodException {
        // given runQuarantinedTests=true
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(true, false);
        assertTrue(getTestAnnotation(quarantinedTransformer, ClassWithASingleQuarantinedMethod.class,
                "testMethod").getEnabled());
        assertTrue(getTestAnnotation(quarantinedTransformer, ClassWithASingleQuarantinedMethod.class,
                "testMethodQuarantined").getEnabled());
    }

    private TestAnnotation getTestAnnotation(
            QuarantinedTestNGAnnotationTransformer quarantinedTestNGAnnotationTransformer,
            Class<?> clazz, String methodName)
            throws NoSuchMethodException {
        TestAnnotation testAnnotation = new TestAnnotation();
        Method testMethod = clazz.getMethod(methodName);
        quarantinedTestNGAnnotationTransformer
                .transform(testAnnotation, clazz, null, testMethod);
        return testAnnotation;
    }

    @Test
    public void shouldEvaluateConstructorAnnotation() throws NoSuchMethodException {
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(false, false);
        assertFalse(getTestAnnotationForConstructor(quarantinedTransformer, ClassWithQuarantinedConstructor.class)
                .getEnabled());
    }

    @Test
    public void shouldEvaluateConstructorAnnotationWhenRunOnlyQuarantinedTests() throws NoSuchMethodException {
        // given runOnlyQuarantinedTests=true
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(false, true);
        assertTrue(getTestAnnotationForConstructor(quarantinedTransformer, ClassWithQuarantinedConstructor.class)
                .getEnabled());
    }

    @Test
    public void shouldEvaluateConstructorAnnotationWhenRunQuarantinedTests() throws NoSuchMethodException {
        // given runQuarantinedTests=true
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(true, false);
        assertTrue(getTestAnnotationForConstructor(quarantinedTransformer, ClassWithQuarantinedConstructor.class)
                .getEnabled());
    }

    @Test
    public void shouldEvaluateMissingConstructorAnnotation() throws NoSuchMethodException {
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(false, false);
        assertTrue(getTestAnnotationForConstructor(quarantinedTransformer, ClassWithASingleQuarantinedMethod.class)
                .getEnabled());
    }

    @Test
    public void shouldEvaluateMissingConstructorAnnotationWhenRunOnlyQuarantinedTests() throws NoSuchMethodException {
        // given runOnlyQuarantinedTests=true
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(false, true);
        assertFalse(getTestAnnotationForConstructor(quarantinedTransformer, ClassWithASingleQuarantinedMethod.class)
                .getEnabled());
    }

    @Test
    public void shouldEvaluateMissingConstructorAnnotationWhenRunQuarantinedTests() throws NoSuchMethodException {
        // given runQuarantinedTests=true
        QuarantinedTestNGAnnotationTransformer quarantinedTransformer =
                new QuarantinedTestNGAnnotationTransformer(true, false);
        assertTrue(getTestAnnotationForConstructor(quarantinedTransformer, ClassWithASingleQuarantinedMethod.class)
                .getEnabled());
    }


    private TestAnnotation getTestAnnotationForConstructor(
            QuarantinedTestNGAnnotationTransformer quarantinedTestNGAnnotationTransformer,
            Class<?> clazz)
            throws NoSuchMethodException {
        TestAnnotation testAnnotation = new TestAnnotation();
        Constructor<?> constructor = clazz.getConstructor();
        quarantinedTestNGAnnotationTransformer
                .transform(testAnnotation, clazz, constructor, null);
        return testAnnotation;
    }

}