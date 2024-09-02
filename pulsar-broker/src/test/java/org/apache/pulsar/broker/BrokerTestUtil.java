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
package org.apache.pulsar.broker;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.UUID;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.mockito.Mockito;
import org.slf4j.Logger;

/**
 * Holds util methods used in test.
 */
public class BrokerTestUtil {
    // Generate unique name for different test run.
    public static String newUniqueName(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    /**
     * Creates a Mockito spy directly without an intermediate instance to spy.
     * This is to address flaky test issue where a spy created with a given instance fails with
     * {@link org.mockito.exceptions.misusing.WrongTypeOfReturnValue} exception.
     * The spy is stub-only which does not record method invocations.
     *
     * @param classToSpy the class to spy
     * @param args the constructor arguments to use when creating the spy instance
     * @return a spy of the provided class created with given constructor arguments
     */
    public static <T> T spyWithClassAndConstructorArgs(Class<T> classToSpy, Object... args) {
        return Mockito.mock(classToSpy, Mockito.withSettings()
                .useConstructor(args)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                .stubOnly());
    }

    /**
     * Creates a Mockito spy directly without an intermediate instance to spy.
     * This is to address flaky test issue where a spy created with a given instance fails with
     * {@link org.mockito.exceptions.misusing.WrongTypeOfReturnValue} exception.
     * The spy records method invocations.
     *
     * @param classToSpy the class to spy
     * @param args the constructor arguments to use when creating the spy instance
     * @return a spy of the provided class created with given constructor arguments
     */
    public static <T> T spyWithClassAndConstructorArgsRecordingInvocations(Class<T> classToSpy, Object... args) {
        return Mockito.mock(classToSpy, Mockito.withSettings()
                .useConstructor(args)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    }

    /**
     * Create a Mockito spy that is stub-only which does not record method invocations,
     * thus saving memory but disallowing verification of invocations.
     *
     * @param object to spy on
     * @return a spy of the real object
     * @param <T> type of object
     */
    public static <T> T spyWithoutRecordingInvocations(T object) {
        return Mockito.mock((Class<T>) object.getClass(), Mockito.withSettings()
                .spiedInstance(object)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                .stubOnly());
    }

    /**
     * Uses Jackson to create a JSON string for the given object
     * @param object to convert to JSON
     * @return JSON string
     */
    public static String toJson(Object object) {
        ObjectWriter writer = ObjectMapperFactory.getMapper().writer();
        StringWriter stringWriter = new StringWriter();
        try (JsonGenerator generator = writer.createGenerator(stringWriter).useDefaultPrettyPrinter()) {
            generator.writeObject(object);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return stringWriter.toString();
    }

    /**
     * Logs the topic stats and internal stats for the given topic
     * @param logger logger to use
     * @param pulsarAdmin PulsarAdmin client to use
     * @param topic topic name
     */
    public static void logTopicStats(Logger logger, PulsarAdmin pulsarAdmin, String topic) {
        try {
            logger.info("[{}] stats: {}", topic, toJson(pulsarAdmin.topics().getStats(topic)));
            logger.info("[{}] internalStats: {}", topic,
                    toJson(pulsarAdmin.topics().getInternalStats(topic, true)));
        } catch (PulsarAdminException e) {
            logger.warn("Failed to get stats for topic {}", topic, e);
        }
    }

}
