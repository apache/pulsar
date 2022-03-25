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
package org.apache.pulsar.sql.presto;

import com.google.common.collect.Sets;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.testing.TestingConnectorContext;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test read chunked messages.
 */
@Test
@Slf4j
public class TestReadChunkedMessages extends MockedPulsarServiceBaseTest {

    private final static int MAX_MESSAGE_SIZE = 1024 * 1024;

    @EqualsAndHashCode
    @Data
    static class Movie {
        private String name;
        private Long publishTime;
        private byte[] binaryData;
    }

    @EqualsAndHashCode
    @Data
    static class MovieMessage {
        private Movie movie;
        private String messageId;
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setMaxMessageSize(MAX_MESSAGE_SIZE);
        conf.setManagedLedgerMaxEntriesPerLedger(5);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        // so that clients can test short names
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void queryTest() throws Exception {
        String topic = "chunk-topic";
        TopicName topicName = TopicName.get(topic);
        int messageCnt = 20;
        Set<MovieMessage> messageSet = prepareChunkedData(topic, messageCnt);
        SchemaInfo schemaInfo = Schema.AVRO(Movie.class).getSchemaInfo();

        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        connectorConfig.setWebServiceUrl(pulsar.getWebServiceAddress());
        PulsarSplitManager pulsarSplitManager = new PulsarSplitManager(new PulsarConnectorId("1"), connectorConfig);
        Collection<PulsarSplit> splits = pulsarSplitManager.getSplitsForTopic(
                topicName.getPersistenceNamingEncoding(),
                pulsar.getManagedLedgerFactory(),
                new ManagedLedgerConfig(),
                3,
                new PulsarTableHandle("1", topicName.getNamespace(), topic, topic),
                schemaInfo,
                topic,
                TupleDomain.all(),
                null);

        List<PulsarColumnHandle> columnHandleList = TestPulsarConnector.getColumnColumnHandles(
                topicName, schemaInfo, PulsarColumnHandle.HandleKeyValueType.NONE, true);
        ConnectorContext prestoConnectorContext = new TestingConnectorContext();

        for (PulsarSplit split : splits) {
            queryAndCheck(columnHandleList, split, connectorConfig, prestoConnectorContext, messageSet);
        }
        Assert.assertTrue(messageSet.isEmpty());
    }

    private Set<MovieMessage> prepareChunkedData(String topic, int messageCnt) throws PulsarClientException, InterruptedException {
        pulsarClient.newConsumer(Schema.AVRO(Movie.class))
                .topic(topic)
                .subscriptionName("sub")
                .subscribe()
                .close();
        Producer<Movie> producer = pulsarClient.newProducer(Schema.AVRO(Movie.class))
                .topic(topic)
                .enableBatching(false)
                .enableChunking(true)
                .create();
        Set<MovieMessage> messageSet = new LinkedHashSet<>();
        CountDownLatch countDownLatch = new CountDownLatch(messageCnt);
        for (int i = 0; i < messageCnt; i++) {
            final double dataTimes = (i % 5) * 0.5;
            byte[] movieBinaryData = RandomUtils.nextBytes((int) (MAX_MESSAGE_SIZE * dataTimes));
            final int length = movieBinaryData.length;
            final int index = i;

            Movie movie = new Movie();
            movie.setName("movie-" + i);
            movie.setPublishTime(System.currentTimeMillis());
            movie.setBinaryData(movieBinaryData);
            producer.newMessage().value(movie).sendAsync()
                    .whenComplete((msgId, throwable) -> {
                        if (throwable != null) {
                            log.error("Failed to produce message.", throwable);
                            countDownLatch.countDown();
                            return;
                        }
                        MovieMessage movieMessage = new MovieMessage();
                        movieMessage.setMovie(movie);
                        MessageIdImpl messageId = (MessageIdImpl) msgId;
                        movieMessage.setMessageId("(" + messageId.getLedgerId() + "," + messageId.getEntryId() + ",0)");
                        messageSet.add(movieMessage);
                        countDownLatch.countDown();
                    });
        }
        countDownLatch.await();
        Assert.assertEquals(messageCnt, messageSet.size());
        producer.close();
        return messageSet;
    }

    private void queryAndCheck(List<PulsarColumnHandle> columnHandleList,
                               PulsarSplit split,
                               PulsarConnectorConfig connectorConfig,
                               ConnectorContext prestoConnectorContext,
                               Set<MovieMessage> messageSet) {
        PulsarRecordCursor pulsarRecordCursor = new PulsarRecordCursor(
                columnHandleList, split, connectorConfig, pulsar.getManagedLedgerFactory(),
                new ManagedLedgerConfig(), new PulsarConnectorMetricsTracker(new NullStatsProvider()),
                new PulsarDispatchingRowDecoderFactory(prestoConnectorContext.getTypeManager()));

        AtomicInteger receiveMsgCnt = new AtomicInteger(messageSet.size());
        while (pulsarRecordCursor.advanceNextPosition()) {
            Movie movie = new Movie();
            MovieMessage movieMessage = new MovieMessage();
            movieMessage.setMovie(movie);
            for (int i = 0; i < columnHandleList.size(); i++) {
                switch (columnHandleList.get(i).getName()) {
                    case "binaryData":
                        movie.setBinaryData(pulsarRecordCursor.getSlice(i).getBytes());
                        break;
                    case "name":
                        movie.setName(new String(pulsarRecordCursor.getSlice(i).getBytes()));
                        break;
                    case "publishTime":
                        movie.setPublishTime(pulsarRecordCursor.getLong(i));
                        break;
                    case "__message_id__":
                        movieMessage.setMessageId(new String(pulsarRecordCursor.getSlice(i).getBytes()));
                    default:
                        // do nothing
                        break;
                }
            }

            Assert.assertTrue(messageSet.contains(movieMessage));
            messageSet.remove(movieMessage);
            receiveMsgCnt.decrementAndGet();
        }
    }

}
