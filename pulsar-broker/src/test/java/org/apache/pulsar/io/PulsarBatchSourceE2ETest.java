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
package org.apache.pulsar.io;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarIOBatchDataGeneratorNar;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.Set;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.BatchSourceConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.functions.utils.FunctionCommon;

import org.apache.pulsar.functions.worker.PulsarFunctionTestUtils;
import org.apache.pulsar.io.batchdiscovery.ImmediateTriggerer;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Test(groups = "broker-io")
public class PulsarBatchSourceE2ETest extends AbstractPulsarE2ETest {

    private void testPulsarBatchSourceStats(String jarFilePathUrl) throws Exception {
    	final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String sourceName = "PulsarBatchSource";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        SourceConfig sourceConfig = createSourceConfig(tenant, namespacePortion, sourceName, sinkTopic);
        sourceConfig.setBatchSourceConfig(createBatchSourceConfig());

        retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic).getPublishers().size() == 1);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 10, 150);

        final String sinkTopic2 = "persistent://" + replNamespace + "/output-" + sourceName;
        sourceConfig.setTopicName(sinkTopic2);

        if (jarFilePathUrl.startsWith(Utils.BUILTIN)) {
          sourceConfig.setArchive(jarFilePathUrl);
          admin.sources().createSource(sourceConfig, jarFilePathUrl);
        } else {
          admin.sources().createSourceWithUrl(sourceConfig, jarFilePathUrl);
        }

        retryStrategically((test) -> {
            try {
                TopicStats sourceStats = admin.topics().getStats(sinkTopic2);
                return sourceStats.getPublishers().size() == 1
                        && sourceStats.getPublishers().get(0).getMetadata() != null
                        && sourceStats.getPublishers().get(0).getMetadata().containsKey("id")
                        && sourceStats.getPublishers().get(0).getMetadata().get("id").equals(String.format("%s/%s/%s", tenant, namespacePortion, sourceName));
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        TopicStats sourceStats = admin.topics().getStats(sinkTopic2);
        assertEquals(sourceStats.getPublishers().size(), 1);
        assertNotNull(sourceStats.getPublishers().get(0).getMetadata());
        assertTrue(sourceStats.getPublishers().get(0).getMetadata().containsKey("id"));
        assertEquals(sourceStats.getPublishers().get(0).getMetadata().get("id"), String.format("%s/%s/%s", tenant, namespacePortion, sourceName));

        retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic2).getPublishers().size() == 1) && (admin.topics().getInternalStats(sinkTopic2, false).numberOfEntries > 4);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertEquals(admin.topics().getStats(sinkTopic2).getPublishers().size(), 1);

        String prometheusMetrics = PulsarFunctionTestUtils.getPrometheusMetrics(pulsar.getListenPortHTTP().get());
        log.info("prometheusMetrics: {}", prometheusMetrics);

        Map<String, PulsarFunctionTestUtils.Metric> metrics = PulsarFunctionTestUtils.parseMetrics(prometheusMetrics);
        PulsarFunctionTestUtils.Metric m = metrics.get("pulsar_source_received_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_source_received_1min_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_source_written_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_source_written_1min_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_source_source_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_source_source_exceptions_1min_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_source_system_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_source_system_exceptions_1min_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_source_last_invocation");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertTrue(m.value > 0.0);

        tempDirectory.assertThatFunctionDownloadTempFilesHaveBeenDeleted();
        admin.sources().deleteSource(tenant, namespacePortion, sourceName);
    }

    @Test(timeOut = 20000, groups = "builtin")
    public void testPulsarBatchSourceStatsBuiltin() throws Exception {
        String jarFilePathUrl = String.format("%s://batch-data-generator", Utils.BUILTIN);
        testPulsarBatchSourceStats(jarFilePathUrl);
    }

    @Test(timeOut = 20000)
    private void testPulsarBatchSourceStatsWithFile() throws Exception {
    	String jarFilePathUrl = getPulsarIOBatchDataGeneratorNar().toURI().toString();
    	testPulsarBatchSourceStats(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    private void testPulsarBatchSourceStatsWithUrl() throws Exception {
    	testPulsarBatchSourceStats(fileServer.getUrl("/pulsar-io-batch-data-generator.nar"));
    }

    private static SourceConfig createSourceConfig(String tenant, String namespace, String functionName, String sinkTopic) {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(functionName);
        sourceConfig.setParallelism(1);
        sourceConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sourceConfig.setTopicName(sinkTopic);
        return sourceConfig;
    }
    
    private static BatchSourceConfig createBatchSourceConfig() {
        return BatchSourceConfig.builder()
                 .discoveryTriggererClassName(ImmediateTriggerer.class.getName())
                 .build();
    }
}
