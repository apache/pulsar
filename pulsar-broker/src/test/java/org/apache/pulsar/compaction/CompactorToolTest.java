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
package org.apache.pulsar.compaction;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;
import com.beust.jcommander.Parameter;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.CmdGenerateDocs;
import org.testng.annotations.Test;

/**
 * CompactorTool Tests.
 */
public class CompactorToolTest {

    /**
     * Test broker-tool generate docs
     *
     * @throws Exception
     */
    @Test
    public void testGenerateDocs() throws Exception {
        PrintStream oldStream = System.out;
        try {
            ByteArrayOutputStream baoStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baoStream));

            Class argumentsClass = Class.forName("org.apache.pulsar.compaction.CompactorTool$Arguments");

            Constructor constructor = argumentsClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            Object obj = constructor.newInstance();

            CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
            cmd.addCommand("compact-topic", obj);
            cmd.run(null);

            String message = baoStream.toString();

            Field[] fields = argumentsClass.getDeclaredFields();
            for (Field field : fields) {
                boolean fieldHasAnno = field.isAnnotationPresent(Parameter.class);
                if (fieldHasAnno) {
                    Parameter fieldAnno = field.getAnnotation(Parameter.class);
                    String[] names = fieldAnno.names();
                    String nameStr = Arrays.asList(names).toString();
                    nameStr = nameStr.substring(1, nameStr.length() - 1);
                    assertTrue(message.indexOf(nameStr) > 0);
                }
            }
        } finally {
            System.setOut(oldStream);
        }
    }

    @Test
    public void testUseTlsUrlWithPEM() throws PulsarClientException {
        ServiceConfiguration serviceConfiguration = spy(ServiceConfiguration.class);
        serviceConfiguration.setBrokerServicePortTls(Optional.of(6651));
        serviceConfiguration.setBrokerClientTlsEnabled(true);
        serviceConfiguration.setProperties(new Properties());

        @Cleanup
        PulsarClient ignored = CompactorTool.createClient(serviceConfiguration);

        verify(serviceConfiguration, times(1)).isBrokerClientTlsEnabled();
        verify(serviceConfiguration, times(1)).isTlsAllowInsecureConnection();
        verify(serviceConfiguration, times(1)).getBrokerClientKeyFilePath();
        verify(serviceConfiguration, times(1)).getBrokerClientTrustCertsFilePath();
        verify(serviceConfiguration, times(1)).getBrokerClientCertificateFilePath();
    }

    @Test
    public void testUseTlsUrlWithKeystore() throws PulsarClientException {
        ServiceConfiguration serviceConfiguration = spy(ServiceConfiguration.class);
        serviceConfiguration.setBrokerServicePortTls(Optional.of(6651));
        serviceConfiguration.setBrokerClientTlsEnabled(true);
        serviceConfiguration.setBrokerClientTlsEnabledWithKeyStore(true);
        serviceConfiguration.setBrokerClientTlsTrustStore(MockedPulsarServiceBaseTest.BROKER_KEYSTORE_FILE_PATH);

        serviceConfiguration.setProperties(new Properties());

        @Cleanup
        PulsarClient ignored = CompactorTool.createClient(serviceConfiguration);

        verify(serviceConfiguration, times(1)).isBrokerClientTlsEnabled();
        verify(serviceConfiguration, times(1)).isBrokerClientTlsEnabledWithKeyStore();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsKeyStore();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsKeyStorePassword();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsKeyStoreType();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsTrustStore();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsTrustStorePassword();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsTrustStoreType();
    }
}
