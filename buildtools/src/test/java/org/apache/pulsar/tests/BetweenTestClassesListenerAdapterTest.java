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
package org.apache.pulsar.tests;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.IClass;
import org.testng.ITestClass;
import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.TestNG;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import org.testng.internal.IParameterInfo;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

public class BetweenTestClassesListenerAdapterTest {
    private TestBetweenTestClassesListener listener;

    @BeforeMethod
    public void setUp() {
        listener = new TestBetweenTestClassesListener();
        listener.reset();
    }

    @Test
    public void testListenerWithNoAfterClassMethods() {
        runTestNGWithClass(NoAfterClassMethods.class);
        verifyListenerCalledForClass(NoAfterClassMethods.class);
    }

    @Test
    public void testListenerWithOneAfterClassMethod() {
        runTestNGWithClass(OneAfterClassMethod.class);
        verifyListenerCalledForClass(OneAfterClassMethod.class);
    }

    @Test
    public void testListenerWithMultipleAfterClassMethods() {
        runTestNGWithClass(MultipleAfterClassMethods.class);
        verifyListenerCalledForClass(MultipleAfterClassMethods.class);
    }

    @Test
    public void testListenerWithDisabledAfterClassMethod() {
        runTestNGWithClass(DisabledAfterClassMethod.class);
        verifyListenerCalledForClass(DisabledAfterClassMethod.class);
    }

    @Test
    public void testListenerWithFailingAfterClassMethods() {
        runTestNGWithClass(FailingAfterClassMethod.class);
        verifyListenerCalledForClass(FailingAfterClassMethod.class);
    }

    @Test
    public void testListenerWithTimeoutAndAfterClassMethod() {
        runTestNGWithClasses(1, TimeoutAndAfterClassMethod.class);
        verifyListenerCalledForClass(TimeoutAndAfterClassMethod.class);
    }

    @Test
    public void testListenerWithFactoryMethod() {
        runTestNGWithClass(FactoryMethodCase.class);
        String className = FactoryMethodCase.class.getName();
        assertEquals(1, listener.getClassesCalled().size(),
                "Listener should be called exactly once");
        assertEquals(className, listener.getClassesCalled().get(0).getName(),
                "Listener should be called for the correct class");
    }

    @Test
    public void testListenerWithFactoryMethodWithoutAfterClassMethods() {
        runTestNGWithClass(FactoryMethodCaseWithoutAfterClass.class);
        String className = FactoryMethodCaseWithoutAfterClass.class.getName();
        assertEquals(1, listener.getClassesCalled().size(),
                "Listener should be called exactly once");
        assertEquals(className, listener.getClassesCalled().get(0).getName(),
                "Listener should be called for the correct class");
    }

    @Test
    public void testListenerWithMultipleTestClasses() {
        runTestNGWithClasses(0,
                NoAfterClassMethods.class,
                OneAfterClassMethod.class,
                FailingAfterClassMethod.class,
                MultipleAfterClassMethods.class);

        assertEquals(4, listener.getClassesCalled().size());

        List<String> actualClassNames = listener.getClassesCalled().stream()
                .map(IClass::getName)
                .collect(Collectors.toList());

        assertTrue(actualClassNames.contains(NoAfterClassMethods.class.getName()));
        assertTrue(actualClassNames.contains(OneAfterClassMethod.class.getName()));
        assertTrue(actualClassNames.contains(MultipleAfterClassMethods.class.getName()));
        assertTrue(actualClassNames.contains(FailingAfterClassMethod.class.getName()));
    }

    private void verifyListenerCalledForClass(Class<?> clazz) {
        String className = clazz.getName();
        assertEquals(1, listener.getClassesCalled().size(),
                "Listener should be called exactly once");
        assertEquals(className, listener.getClassesCalled().get(0).getName(),
                "Listener should be called for the correct class");
    }

    private void runTestNGWithClass(Class<?> testClass) {
        runTestNGWithClasses(0, testClass);
    }

    // programmatically test TestNG listener with some classes
    private void runTestNGWithClasses(int expectedFailureCount, Class<?>... testClasses) {
        XmlSuite suite = new XmlSuite();
        suite.setName("Programmatic Suite");

        for (Class<?> cls : testClasses) {
            // create a new XmlTest for each class so that this simulates the behavior of maven-surefire-plugin
            XmlTest test = new XmlTest(suite);
            test.setName("Programmatic Test for " + cls.getName());
            List<XmlClass> xmlClasses = new ArrayList<>();
            xmlClasses.add(new XmlClass(cls));
            test.setXmlClasses(xmlClasses);
        }

        List<XmlSuite> suites = new ArrayList<>();
        suites.add(suite);

        TestNG tng = new TestNG();
        tng.setXmlSuites(suites);
        tng.addListener(listener);
        tng.addListener(new TestLoggingListener());
        // set verbose output for debugging
        tng.setVerbose(2);
        AtomicInteger failureCounter = new AtomicInteger();
        tng.addListener(new ITestListener() {
            @Override
            public void onTestFailure(ITestResult result) {
                failureCounter.incrementAndGet();
            }
        });
        tng.run();
        assertEquals(failureCounter.get(), expectedFailureCount, "TestNG run should complete successfully");
    }

    // Test implementation of the abstract listener
    private class TestBetweenTestClassesListener extends BetweenTestClassesListenerAdapter {
        private final List<ITestClass> classesCalled = new ArrayList<>();

        @Override
        protected void onBetweenTestClasses(List<ITestClass> testClasses) {
            assertEquals(testClasses.size(), 1);
            ITestClass testClass = testClasses.get(0);
            System.out.println("onBetweenTestClasses " + testClass);
            classesCalled.add(testClass);
            closeTestInstance(testClass);
        }

        private void closeTestInstance(ITestClass testClass) {
            Arrays.stream(testClass.getInstances(false))
                    .map(instance -> instance instanceof IParameterInfo
                            ? ((IParameterInfo) instance).getInstance() : instance)
                    .filter(AutoCloseable.class::isInstance)
                    .map(AutoCloseable.class::cast)
                    .forEach(autoCloseable -> {
                        try {
                            autoCloseable.close();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        public List<ITestClass> getClassesCalled() {
            return classesCalled;
        }

        public void reset() {
            classesCalled.clear();
        }
    }

    private static class TestLoggingListener implements ITestListener {
        @Override
        public void onTestStart(ITestResult result) {
            System.out.println("Test started: " + result.getName() + " in instance " + result.getInstance());
        }
    }

    public static class TestRetryAnalyzer extends RetryAnalyzer {
        public TestRetryAnalyzer() {
            // retry once
            setCount(1);
        }
    }

    private static class CloseableBase implements AutoCloseable {
        protected boolean closed;

        protected void checkNotClosed() {
            Assert.assertFalse(closed);
        }

        public void close() {
            closed = true;
        }
    }

    private static class Base extends CloseableBase {
        int counter = 0;

        protected void failOnFirstExecution() {
            if (counter++ == 0) {
                throw new IllegalStateException("Simulated failure");
            }
        }

        @Test(retryAnalyzer = TestRetryAnalyzer.class)
        public void testMethod() {
            checkNotClosed();
            failOnFirstExecution();
        }

        @AfterMethod(alwaysRun = true)
        public void afterMethodInBase() {
            checkNotClosed();
        }

        @AfterClass(alwaysRun = true)
        public void afterClassInBase() {
            checkNotClosed();
        }
    }

    private static class NoAfterClassMethods extends Base {

    }

    private static class OneAfterClassMethod extends Base {
        @AfterClass
        public void afterClass() {
            checkNotClosed();
        }
    }

    private static class TimeoutAndAfterClassMethod extends Base {
        @Override
        @Test(timeOut = 100)
        public void testMethod() {
            checkNotClosed();
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @AfterClass(alwaysRun = true)
        public void afterClass() {
            checkNotClosed();
        }
    }

    private static class MultipleAfterClassMethods extends Base {
        private static final AtomicInteger afterClassCounter = new AtomicInteger(0);

        @AfterClass
        public void afterClass1() {
            checkNotClosed();
            afterClassCounter.incrementAndGet();
        }

        @AfterClass
        public void afterClass2() {
            checkNotClosed();
            afterClassCounter.incrementAndGet();
        }
    }

    private static class DisabledAfterClassMethod extends Base {
        @AfterClass(enabled = false)
        public void disabledAfterClass() {}
    }

    private static class FailingAfterClassMethod extends Base {
        @AfterClass
        public void failingAfterClass() {
            checkNotClosed();
            throw new RuntimeException("Simulated failure");
        }
    }

    protected static class FactoryMethodCase extends Base {
        private final int id;

        private FactoryMethodCase(int id) {
            this.id = id;
        }

        @Factory
        public static Object[] createTestInstances() {
            return new Object[]{
                    new FactoryMethodCase(1),
                    new FactoryMethodCase(2)
            };
        }

        @DataProvider
        public Object[] idDataProvider() {
            return new Object[]{ id };
        }

        @Test(dataProvider = "idDataProvider")
        public void testWithDataProvider(int id) {
            checkNotClosed();
            Assert.assertEquals(this.id, id);
        }

        @AfterClass
        public void afterClass() {
            checkNotClosed();
        }

        @Override
        public String toString() {
            return "FactoryMethodCase{" +
                    "id=" + id +
                    '}';
        }
    }

    protected static class FactoryMethodCaseWithoutAfterClass extends CloseableBase {
        private final int id;

        private FactoryMethodCaseWithoutAfterClass(int id) {
            this.id = id;
        }

        @Factory
        public static Object[] createTestInstances() {
            return new Object[]{
                    new FactoryMethodCaseWithoutAfterClass(1),
                    new FactoryMethodCaseWithoutAfterClass(2)
            };
        }

        @DataProvider
        public Object[] idDataProvider() {
            return new Object[]{ id };
        }

        @Test(dataProvider = "idDataProvider")
        public void testWithDataProvider(int id) {
            checkNotClosed();
            Assert.assertEquals(this.id, id);
        }

        @Override
        public String toString() {
            return "FactoryMethodCaseWithoutAfterClass{" +
                    "id=" + id +
                    '}';
        }
    }
}