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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import org.testng.annotations.Test;

public class FailureCauseTest {
    @Test
    public void namesTheFailureItsDeepestMessageAndItsRootCause() {
        String log = """
                Picked up JAVA_TOOL_OPTIONS: -Xms128m -Xmx512m
                READY applications=1 clients=10
                java.lang.IllegalStateException: Cannot restart IoT client
                \tat org.apache.pulsar.tests.performance.tools.TelemetryConsumer.call(TelemetryConsumer.java:77)
                Caused by: org.apache.pulsar.client.api.PulsarClientException: \
                java.util.concurrent.ExecutionException: Failed to resolve 'broker-0' [A(1)]
                \tat org.apache.pulsar.client.api.PulsarClientException.unwrap(PulsarClientException.java:1143)
                Caused by: java.net.UnknownHostException: Failed to resolve 'broker-0' [A(1)]
                \t... 23 more
                Caused by: io.netty.resolver.dns.DnsNameResolverException: [13290: /127.0.0.11:53] \
                DefaultDnsQuestion(broker-0. IN A) failed to send a query '13290' via UDP (no stack trace available)
                Caused by: io.netty.channel.StacklessClosedChannelException
                \tat io.netty.channel.AbstractChannel$AbstractUnsafe.write(Object, ChannelPromise)(Unknown Source)
                """;

        assertThat(FailureCause.of(log)).isEqualTo("IllegalStateException: Cannot restart IoT client, "
                + "caused by DnsNameResolverException: [13290: /127.0.0.11:53] DefaultDnsQuestion(broker-0. IN A) "
                + "failed to send a query '13290' via UDP (no stack trace available), "
                + "caused by StacklessClosedChannelException");
    }

    @Test
    public void ignoresFailuresAfterTheFirst() {
        String log = """
                java.lang.IllegalStateException: Telemetry send failed
                Caused by: java.util.concurrent.TimeoutException: No reply in 30 s
                java.io.IOException: Closing the client failed
                """;

        assertThat(FailureCause.of(log)).isEqualTo(
                "IllegalStateException: Telemetry send failed, caused by TimeoutException: No reply in 30 s");
    }

    @Test
    public void fallsBackToTheLastLineWithoutAnException() {
        assertThat(FailureCause.of("READY applications=1 clients=10\nReceived 99 of 100 messages\n\n"))
                .isEqualTo("Received 99 of 100 messages");
        assertThat(FailureCause.of("")).isNull();
    }
}
