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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@code @Quarantined} is used to signal that the annotated test class or
 * test method is currently <em>quarantined</em> since it is flaky.
 *
 * <p>Quarantined tests aren't run by default. Running quarantined tests happens
 * by setting the {@code runQuarantinedTests} system property to true.
 *
 * <p>{@code @Quarantined} may optionally be declared with a {@linkplain #value
 * reason} to document why the annotated test class or test method is quarantined.
 *
 * <p>When applied at the class level, all test methods within that class
 * are all quarantined.
 *
 * <p>When applied at the method level, the quarantine is applied to the method.
 *
 * <p>This annotation will be effective when {@link QuarantinedTestNGAnnotationTransformer}
 * class is registered as a TestNG listener.
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Quarantined {
    /**
     * The reason this annotated test class or test method is quarantined.
     */
    String value() default "";
}
