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

import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricDoubleSumValue;
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.Attributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.stats.OpenTelemetryTopicStats;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-compaction")
public class EventTimeOrderCompactorTest extends CompactorTest {

  private EventTimeOrderCompactor compactor;

  @BeforeMethod
  @Override
  public void setup() throws Exception {
    super.setup();
    compactor = new EventTimeOrderCompactor(conf, pulsarClient, bk, compactionScheduler);
  }

  @Override
  protected long compact(String topic) throws ExecutionException, InterruptedException {
    return compactor.compact(topic).get();
  }

  @Override
  protected Compactor getCompactor() {
    return compactor;
  }

  @Test
  public void testCompactedOutByEventTime() throws Exception {
    String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/use/my-ns/testCompactedOutByEventTime");
    this.restartBroker();

    @Cleanup
    Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
        .enableBatching(true).topic(topicName).batchingMaxMessages(3).create();

    producer.newMessage().key("K1").value("V1").eventTime(1L).sendAsync();
    producer.newMessage().key("K2").value("V2").eventTime(1L).sendAsync();
    producer.newMessage().key("K2").value(null).eventTime(2L).sendAsync();
    producer.flush();

    admin.topics().triggerCompaction(topicName);

    Awaitility.await().untilAsserted(() -> {
      Assert.assertEquals(admin.topics().compactionStatus(topicName).status,
          LongRunningProcessStatus.Status.SUCCESS);
    });

    var attributes = Attributes.builder()
        .put(OpenTelemetryAttributes.PULSAR_DOMAIN, "persistent")
        .put(OpenTelemetryAttributes.PULSAR_TENANT, "my-property")
        .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "my-property/use/my-ns")
        .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicName)
        .build();
    var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
    assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_REMOVED_COUNTER, attributes, 1);
    assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_OPERATION_COUNTER, Attributes.builder()
            .putAll(attributes)
            .put(OpenTelemetryAttributes.PULSAR_COMPACTION_STATUS, "success")
            .build(),
        1);
    assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_OPERATION_COUNTER, Attributes.builder()
            .putAll(attributes)
            .put(OpenTelemetryAttributes.PULSAR_COMPACTION_STATUS, "failure")
            .build(),
        0);
    assertMetricDoubleSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_DURATION_SECONDS, attributes,
        actual -> assertThat(actual).isPositive());
    assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_BYTES_IN_COUNTER, attributes,
        actual -> assertThat(actual).isPositive());
    assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_BYTES_OUT_COUNTER, attributes,
        actual -> assertThat(actual).isPositive());
    assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_ENTRIES_COUNTER, attributes, 1);
    assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_BYTES_COUNTER, attributes,
        actual -> assertThat(actual).isPositive());

    producer.newMessage().key("K1").eventTime(2L).value("V1-2").sendAsync();
    producer.flush();

    admin.topics().triggerCompaction(topicName);

    Awaitility.await().untilAsserted(() -> {
      Assert.assertEquals(admin.topics().compactionStatus(topicName).status,
          LongRunningProcessStatus.Status.SUCCESS);
    });

    @Cleanup
    Reader<String> reader = pulsarClient.newReader(Schema.STRING)
        .subscriptionName("reader-test")
        .topic(topicName)
        .readCompacted(true)
        .startMessageId(MessageId.earliest)
        .create();
    while (reader.hasMessageAvailable()) {
      Message<String> message = reader.readNext(3, TimeUnit.SECONDS);
      Assert.assertEquals(message.getEventTime(), 2L);
    }
  }

  @Test
  public void testCompactWithEventTimeAddCompact() throws Exception {
    String topic = "persistent://my-property/use/my-ns/my-topic1";

    @Cleanup
    Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
        .enableBatching(false)
        .messageRoutingMode(MessageRoutingMode.SinglePartition)
        .create();

    Map<String, byte[]> expected = new HashMap<>();

    producer.newMessage()
        .key("a")
        .eventTime(1L)
        .value("A_1".getBytes())
        .send();
    producer.newMessage()
        .key("b")
        .eventTime(1L)
        .value("B_1".getBytes())
        .send();
    producer.newMessage()
        .key("a")
        .eventTime(2L)
        .value("A_2".getBytes())
        .send();
    expected.put("a", "A_2".getBytes());
    expected.put("b", "B_1".getBytes());

    compactAndVerify(topic, new HashMap<>(expected), false);

    producer.newMessage()
        .key("b")
        .eventTime(2L)
        .value("B_2".getBytes())
        .send();
    expected.put("b", "B_2".getBytes());

    compactAndVerify(topic, expected, false);
  }

  @Override
  @Test
  public void testPhaseOneLoopTimeConfiguration() {
    ServiceConfiguration configuration = new ServiceConfiguration();
    configuration.setBrokerServiceCompactionPhaseOneLoopTimeInSeconds(60);
    PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
    ConnectionPool connectionPool = mock(ConnectionPool.class);
    when(mockClient.getCnxPool()).thenReturn(connectionPool);
    EventTimeOrderCompactor compactor = new EventTimeOrderCompactor(configuration, mockClient,
        Mockito.mock(BookKeeper.class), compactionScheduler);
    Assert.assertEquals(compactor.getPhaseOneLoopReadTimeoutInSeconds(), 60);
  }
}
