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
package org.apache.pulsar.tests.integration.nativeimage;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Standalone application compiled to a GraalVM native image by the
 * {@code pulsar-client-native-image} test module. It exercises the Pulsar client (and
 * admin client) against a running broker so the {@code NativeImageSmokeTest} can verify
 * that the native image — built using only the metadata embedded in
 * {@code pulsar-client-original} and {@code pulsar-client-admin-original} — actually works
 * at runtime.
 *
 * <p>Each test case is selected through a CLI subcommand. The application prints a stable
 * marker ({@link #SUCCESS_MARKER} / {@link #FAILURE_MARKER}) to stdout and uses the process
 * exit code (0 = success, non-zero = failure) so the test can assert on both.
 *
 * <p>Usage:
 * <pre>
 *   pulsar-client-native-tester produce-consume --service-url pulsar://host:6650 [--topic name]
 *   pulsar-client-native-tester admin --admin-url http://host:8080
 * </pre>
 */
public final class NativeImageTesterApp {

    /** Printed to stdout when a subcommand completes successfully. */
    static final String SUCCESS_MARKER = "NATIVE_IMAGE_TEST_SUCCESS";
    /** Printed to stdout (prefixing the error) when a subcommand fails. */
    static final String FAILURE_MARKER = "NATIVE_IMAGE_TEST_FAILURE";

    private NativeImageTesterApp() {
    }

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                throw new IllegalArgumentException("missing subcommand; expected 'produce-consume' or 'admin'");
            }
            final String subcommand = args[0];
            switch (subcommand) {
                case "produce-consume":
                    produceConsume(requireOption(args, "--service-url"),
                            optionOrDefault(args, "--topic", "native-image-smoke-test"));
                    break;
                case "admin":
                    admin(requireOption(args, "--admin-url"));
                    break;
                default:
                    throw new IllegalArgumentException("unknown subcommand: " + subcommand);
            }
            System.out.println(SUCCESS_MARKER);
            System.exit(0);
        } catch (Throwable t) {
            System.out.println(FAILURE_MARKER + ": " + t.getClass().getName() + ": " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void produceConsume(String serviceUrl, String topic) throws Exception {
        final String payload = "Hello-from-native-image!";
        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
             Producer<String> producer = client.newProducer(Schema.STRING)
                     .topic(topic)
                     .enableBatching(false)
                     .create();
             Consumer<String> consumer = client.newConsumer(Schema.STRING)
                     .topic(topic)
                     .subscriptionName("native-image-sub")
                     .subscriptionType(SubscriptionType.Exclusive)
                     .ackTimeout(10, TimeUnit.SECONDS)
                     .subscribe()) {

            producer.send(payload);
            Message<String> message = consumer.receive(30, TimeUnit.SECONDS);
            if (message == null) {
                throw new IllegalStateException("did not receive a message within the timeout");
            }
            if (!payload.equals(message.getValue())) {
                throw new IllegalStateException("unexpected message payload: " + message.getValue());
            }
            consumer.acknowledge(message);
            System.out.println("produce-consume: received expected payload");
        }
    }

    private static void admin(String adminUrl) throws Exception {
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            // A read-only call that exercises the Jersey/HK2/Jackson REST stack and
            // deserializes a response, which is what stresses the admin native-image config.
            List<String> clusters = admin.clusters().getClusters();
            List<String> tenants = admin.tenants().getTenants();
            if (clusters == null || tenants == null) {
                throw new IllegalStateException("admin returned a null response");
            }
            System.out.println("admin: clusters=" + clusters + " tenants=" + tenants);
        }
    }

    /** Returns the value following {@code name}, or throws if the option is absent. */
    private static String requireOption(String[] args, String name) {
        String value = optionOrDefault(args, name, null);
        if (value == null) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    /** Returns the value following {@code name}, or {@code defaultValue} if not present. */
    private static String optionOrDefault(String[] args, String name, String defaultValue) {
        for (int i = 1; i < args.length - 1; i++) {
            if (name.equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
