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
package org.apache.pulsar.common.lookup.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.testng.annotations.Test;

public class LookupDataTest {

    @Test
    public void withConstructor() {
        LookupData data = new LookupData("pulsar://localhost:8888", "pulsar://localhost:8884", "http://localhost:8080",
                                         "http://localhost:8081");
        assertEquals(data.getBrokerUrl(), "pulsar://localhost:8888");
        assertEquals(data.getHttpUrl(), "http://localhost:8080");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void serializeToJsonTest() throws Exception {
        LookupData data = new LookupData("pulsar://localhost:8888", "pulsar://localhost:8884", "http://localhost:8080",
                                         "http://localhost:8081");
        ObjectMapper mapper = ObjectMapperFactory.getMapper().getObjectMapper();
        String json = mapper.writeValueAsString(data);

        Map<String, String> jsonMap = mapper.readValue(json, Map.class);

        assertEquals(jsonMap.get("brokerUrl"), "pulsar://localhost:8888");
        assertEquals(jsonMap.get("brokerUrlTls"), "pulsar://localhost:8884");
        assertEquals(jsonMap.get("brokerUrlSsl"), "");
        assertEquals(jsonMap.get("nativeUrl"), "pulsar://localhost:8888");
        assertEquals(jsonMap.get("httpUrl"), "http://localhost:8080");
        assertEquals(jsonMap.get("httpUrlTls"), "http://localhost:8081");
    }

    @Test
    public void testUrlEncoder() {
        final String str = "specialCharacters_+&*%{}() \\/$@#^%";
        final String urlEncoded = Codec.encode(str);
        final String uriEncoded = urlEncoded.replaceAll("//+", "%20");
        assertEquals("specialCharacters_%2B%26*%25%7B%7D%28%29+%5C%2F%24%40%23%5E%25", urlEncoded);
        assertEquals(str, Codec.decode(urlEncoded));
        assertEquals(Codec.decode(urlEncoded), Codec.decode(uriEncoded));
    }

    @Test
    public void testLoadReportSerialization() throws Exception {
        final String simpleLmBrokerUrl = "simple";
        final String simpleLmReportName = "simpleLoadManager";
        final String modularLmBrokerUrl = "modular";
        final SystemResourceUsage simpleLmSystemResourceUsage = new SystemResourceUsage();
        final double usage = 55.0;
        final ResourceUsage resource = new ResourceUsage(usage, 0);
        simpleLmSystemResourceUsage.bandwidthIn = resource;

        LoadReport simpleReport = getSimpleLoadManagerLoadReport(simpleLmBrokerUrl, simpleLmReportName,
                simpleLmSystemResourceUsage);

        LocalBrokerData modularReport = getModularLoadManagerLoadReport(modularLmBrokerUrl, resource);

        LoadManagerReport simpleLoadReport = ObjectMapperFactory.getMapper().reader().readValue(
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(simpleReport), LoadManagerReport.class);
        LoadManagerReport modularLoadReport = ObjectMapperFactory.getMapper().reader().readValue(
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(modularReport), LoadManagerReport.class);

        assertEquals(simpleLoadReport.getWebServiceUrl(), simpleLmBrokerUrl);
        assertTrue(simpleLoadReport instanceof LoadReport);
        assertEquals(((LoadReport) simpleLoadReport).getName(), simpleLmReportName);
        assertEquals(((LoadReport) simpleLoadReport).getSystemResourceUsage().bandwidthIn.usage, usage);

        assertEquals(modularLoadReport.getWebServiceUrl(), modularLmBrokerUrl);
        assertTrue(modularLoadReport instanceof LocalBrokerData);
        assertEquals(((LocalBrokerData) modularLoadReport).getBandwidthIn().usage, usage);

    }

    private LoadReport getSimpleLoadManagerLoadReport(String brokerUrl, String reportName,
            SystemResourceUsage systemResourceUsage) {
        LoadReport report = new LoadReport(brokerUrl, null, null, null);
        report.setName(reportName);
        report.setSystemResourceUsage(systemResourceUsage);
        return report;
    }

    private LocalBrokerData getModularLoadManagerLoadReport(String brokerUrl, ResourceUsage bandwidthIn) {
        LocalBrokerData report = new LocalBrokerData(brokerUrl, null, null, null);
        report.setBandwidthIn(bandwidthIn);
        return report;
    }
}
