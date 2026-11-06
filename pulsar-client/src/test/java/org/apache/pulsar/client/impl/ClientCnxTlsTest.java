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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClientCnxTlsTest {

    @DataProvider
    public Object[][] tlsAuthenticationAlerts() {
        return new Object[][]{
                {"Received fatal alert: certificate_required"},
                {"Received fatal alert: bad_certificate"},
                {"Received fatal alert: unsupported_certificate"},
                {"Received fatal alert: certificate_revoked"},
                {"Received fatal alert: certificate_expired"},
                {"Received fatal alert: certificate_unknown"},
                {"Received fatal alert: unknown_ca"},
                {"error:1000045c:SSL routines:OPENSSL_internal:TLSV1_ALERT_CERTIFICATE_REQUIRED"},
                {"error:10000412:SSL routines:OPENSSL_internal:SSLV3_ALERT_BAD_CERTIFICATE"},
                {"error:10000418:SSL routines:OPENSSL_internal:TLSV1_ALERT_UNKNOWN_CA"}
        };
    }

    @Test(dataProvider = "tlsAuthenticationAlerts")
    public void testTlsAuthenticationAlertIsNotRetriable(String message) throws Exception {
        SSLException alert = new SSLHandshakeException(message);
        assertTlsFailure(alert, true, false);
        // TLS 1.3 can finish the local handshake before the server rejects the client certificate.
        assertTlsFailure(alert, false, false);
    }

    @DataProvider
    public Object[][] otherConnectionFailures() {
        return new Object[][]{
                {new IOException("Connection reset")},
                {new SSLHandshakeException("Remote host terminated the handshake")},
                {new SSLHandshakeException("Received fatal alert: handshake_failure")},
                {new SSLHandshakeException("Received fatal alert: protocol_version")},
                {new SSLHandshakeException("Received fatal alert: bad_certificate_status_response")},
                {new SSLHandshakeException("PKIX path building failed: unable to find valid certification path")},
                {new SSLException("TLS handshake timed out")},
                {new SSLException("certificate_expired")},
                {new SSLException((String) null)},
                {new IOException("Received fatal alert: certificate_required")}
        };
    }

    @Test(dataProvider = "otherConnectionFailures")
    public void testOtherConnectionFailuresRemainRetriable(Throwable cause) throws Exception {
        assertTlsFailure(cause, true, true);
        assertTlsFailure(cause, false, true);
    }

    private void assertTlsFailure(Throwable cause, boolean handshakeEvent, boolean retriable) throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("client-cnx-tls-test"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setKeepAliveIntervalSeconds(0);
        ClientCnx cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
        EmbeddedChannel channel = new EmbeddedChannel(cnx);
        try {
            if (handshakeEvent) {
                channel.pipeline().fireUserEventTriggered(new SslHandshakeCompletionEvent(cause));
            } else {
                channel.pipeline().fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS);
                channel.pipeline().fireExceptionCaught(new DecoderException(cause));
            }
            channel.close().sync();
            Throwable failure = cnx.connectionFuture().handle((result, error) -> error).get(5, TimeUnit.SECONDS);
            assertThat(failure).isNotNull();
            Throwable connectionError = FutureUtil.unwrapCompletionException(failure);
            assertThat(connectionError).isInstanceOf(PulsarClientException.class);
            assertThat(PulsarClientException.isRetriableError(connectionError)).isEqualTo(retriable);
            if (!retriable) {
                assertThat(connectionError.getCause()).isSameAs(cause);
                assertThat(connectionError).hasMessageContaining(cause.getMessage());
            }
        } finally {
            channel.finishAndReleaseAll();
            eventLoop.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
        }
    }
}
