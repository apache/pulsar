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
package org.apache.pulsar.broker.transaction;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.offload.filesystem.impl.FileSystemManagedLedgerOffloader;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.BlobStoreManagedLedgerOffloader;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.domain.Credentials;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

@Slf4j
public class OffloadTxnDataTest extends TransactionTestBase{

    private static final String TENANT = "tnx";
    private static final String NAMESPACE1 = TENANT + "/ns1";
    private static final int NUM_BROKERS = 1;
    private static final int NUM_PARTITIONS = 1;
    private Properties properties = new Properties();

    protected PulsarMockBookKeeper bk;
    protected JCloudBlobStoreProvider provider;
    protected TieredStorageConfiguration config;
    public static final String BUCKET = "pulsar-unittest";

    @Override
    protected void setup() throws Exception {
        this.setBrokerCount(NUM_BROKERS);
        this.internalSetup();
        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length - 1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder()
                .serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant(TENANT,
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), NUM_PARTITIONS);
        pulsarClient.close();
        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();
        // wait tc init success to ready state
        waitForCoordinatorToBeAvailable(NUM_PARTITIONS);
    }

    private FileSystemManagedLedgerOffloader buildFileSystemOffloader() throws IOException {
        OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("offloader").build();
        String basePath = "pulsar";
        final String driver = "fileSystem";
        MiniDFSCluster hdfsCluster;
        String hdfsURI;
        File baseDir = Files.createTempDirectory(basePath).toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();

        hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
        Properties properties = new Properties();
        properties.setProperty("managedLedgerOffloadDriver", driver);
        FileSystemManagedLedgerOffloader fileSystemManagedLedgerOffloader= new FileSystemManagedLedgerOffloader(
                OffloadPoliciesImpl.create(properties),
                scheduler, hdfsURI, basePath);
        fileSystemManagedLedgerOffloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInBytes(7L);
        return fileSystemManagedLedgerOffloader;
    }

    private BlobStoreManagedLedgerOffloader buildBlobstoreOffloader() throws Exception {
        OrderedScheduler scheduler;
        scheduler = OrderedScheduler.newSchedulerBuilder().numThreads(5).name("offloader").build();
        bk = new PulsarMockBookKeeper(scheduler);
        provider = JCloudBlobStoreProvider.TRANSIENT;

        config = getConfiguration(BUCKET);
        assertNotNull(provider);
        provider.validate(config);
        BlobStore blobStore = provider.getBlobStore(config);
        Map<String, String> map = new HashMap<>();
        map.put("managedLedgerOffloadThresholdInBytes", "1");
        TieredStorageConfiguration mockedConfig =
                mock(TieredStorageConfiguration.class, delegatesTo(getConfiguration(BUCKET)));
        Mockito.doReturn(blobStore).when(mockedConfig).getBlobStore(); // Use the REAL blobStore
        Mockito.doReturn("azureblob").when(mockedConfig).getDriver();
        Mockito.doReturn(map).when(mockedConfig).getConfigProperties();

        BlobStoreManagedLedgerOffloader blobStoreManagedLedgerOffloader =
                BlobStoreManagedLedgerOffloader.create(mockedConfig, new HashMap<String,String>(), scheduler);
        return blobStoreManagedLedgerOffloader;
    }


    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testFileSystemOffloadTxnData() throws Exception {
        FileSystemManagedLedgerOffloader fileSystemManagedLedgerOffloader = buildFileSystemOffloader();
        fileSystemManagedLedgerOffloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInBytes(1L);
        setLedgerOffloader(fileSystemManagedLedgerOffloader);
        setMaxEntriesPerLedger(2);
        setProperties(properties);
        setup();
        sendAndOffloadMessages(fileSystemManagedLedgerOffloader);
    }

    @Test
    public void testBlobStoreOffloadTxnData() throws Exception {
        BlobStoreManagedLedgerOffloader blobStoreManagedLedgerOffloader = buildBlobstoreOffloader();
        setMaxEntriesPerLedger(2);
        setLedgerOffloader(blobStoreManagedLedgerOffloader);
        setProperties(properties);
        setup();
        sendAndOffloadMessages(blobStoreManagedLedgerOffloader);
    }

    private void sendAndOffloadMessages(LedgerOffloader ledgerOffloader) throws Exception {
        String topic = "persistent://" + NAMESPACE1 + "/testOffloadTxnData";
        admin.topics().createNonPartitionedTopic(topic);
        Map<String, String> map = new HashMap<>();
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .producerName("testOffload")
                .enableBatching(false)
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(topic)
                .create();
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(topic, false)
                .get().get();
        List<MessageIdImpl> messageIdList = new ArrayList<>();
        map.put("ManagedLedgerName", persistentTopic.getManagedLedger().getName());

        //Offload ordinary messages when transactionBuffer is NoSnapshot.
        messageIdList.add((MessageIdImpl) producer.newMessage(Schema.STRING).value("ordinary message").send());
        messageIdList.add((MessageIdImpl) producer.newMessage(Schema.STRING).value("ordinary message").send());

        MessageIdImpl messageId1 = messageIdList.get(0);
        ReadHandle readHandle = waitOffloadAndGetReadHandle(messageId1, persistentTopic, map, ledgerOffloader);
        LedgerEntries ledgerEntries = readHandle.read(0, 1);
        Iterator<LedgerEntry> ledgerEntryIterator = ledgerEntries.iterator();
        Assert.assertEquals(messageIdList.get(0).getEntryId(), ledgerEntryIterator.next().getEntryId());
        Assert.assertEquals(messageIdList.get(1).getEntryId(), ledgerEntryIterator.next().getEntryId());


        //Offload transaction messages. filter aborted messages and txn mark
        Transaction committedTxn= pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        messageIdList.add((MessageIdImpl) producer.newMessage(committedTxn).value("txn message").sendAsync().get());
        committedTxn.commit();
        MessageIdImpl messageId2 = messageIdList.get(2);

        ReadHandle readHandle2 = waitOffloadAndGetReadHandle(messageId2, persistentTopic, map, ledgerOffloader);;
        LedgerEntries ledgerEntries2 = readHandle2.read(0, 1);
        Iterator<LedgerEntry> ledgerEntryIterator2 = ledgerEntries2.iterator();
        LedgerEntry ledgerEntry = ledgerEntryIterator2.next();
        Assert.assertEquals(messageId2.getLedgerId(), ledgerEntry.getLedgerId());
        Assert.assertEquals(messageId2.getEntryId(), ledgerEntry.getEntryId());
        Assert.assertFalse(ledgerEntryIterator2.hasNext());

        Transaction abortedTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        messageIdList.add((MessageIdImpl) producer.newMessage(abortedTxn).value("txn message").sendAsync().get());
        abortedTxn.abort();
        MessageIdImpl messageId3 = messageIdList.get(3);
        ReadHandle readHandle3 = waitOffloadAndGetReadHandle(messageId3, persistentTopic, map, ledgerOffloader);
        LedgerEntries ledgerEntries3 = readHandle3.read(0, 1);
        Iterator<LedgerEntry> ledgerEntryIterator3 = ledgerEntries3.iterator();
        Assert.assertEquals(0, ledgerEntryIterator3.next().getLength());
        Assert.assertFalse(ledgerEntryIterator3.hasNext());

        messageIdList.add((MessageIdImpl) producer.newMessage(Schema.STRING).value("ordinary message").send());
        messageIdList.add((MessageIdImpl) producer.newMessage(Schema.STRING).value("ordinary message").send());
        MessageIdImpl messageId4 = messageIdList.get(4);

        ReadHandle readHandle4 = waitOffloadAndGetReadHandle(messageId4, persistentTopic, map, ledgerOffloader);;
        LedgerEntries ledgerEntries4 = readHandle4.read(0, 1);
        Iterator<LedgerEntry> ledgerEntryIterator4 = ledgerEntries4.iterator();
        Assert.assertEquals(messageIdList.get(4).getEntryId(), ledgerEntryIterator4.next().getEntryId());
        Assert.assertEquals(messageIdList.get(5).getEntryId(), ledgerEntryIterator4.next().getEntryId());

    }

    private ReadHandle waitOffloadAndGetReadHandle(MessageIdImpl messageId, PersistentTopic persistentTopic,
                                                   Map<String, String> map, LedgerOffloader ledgerOffloader)
            throws ExecutionException, InterruptedException {
        //Wait for the automatically triggered offload to be executed completely.
        Awaitility.await().until(() -> {
            MLDataFormats.ManagedLedgerInfo.LedgerInfo info =
                    persistentTopic.getManagedLedger().getLedgerInfo(messageId.getLedgerId()).get();
            return info.getOffloadContext().getComplete();
        });
        MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo3 =
                persistentTopic.getManagedLedger().getLedgerInfo(messageId.getLedgerId()).get();
        UUID uuid3 = new UUID(ledgerInfo3.getOffloadContext().getUidMsb(), ledgerInfo3.getOffloadContext().getUidLsb());
        if (ledgerOffloader instanceof BlobStoreManagedLedgerOffloader) {
            return ledgerOffloader.readOffloaded(messageId.getLedgerId(), uuid3, Collections.emptyMap()).get();
        }
        return ledgerOffloader.readOffloaded(messageId.getLedgerId(), uuid3, map).get();
    }

    protected TieredStorageConfiguration getConfiguration(String bucket) {
        return getConfiguration(bucket, null);
    }

    protected TieredStorageConfiguration getConfiguration(String bucket, Map<String, String> additionalConfig) {
        Map<String, String> metaData = new HashMap<String, String>();
        if (additionalConfig != null) {
            metaData.putAll(additionalConfig);
        }
        metaData.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, provider.getDriver());
        metaData.put(getConfigKey(TieredStorageConfiguration.METADATA_FIELD_REGION), "");
        metaData.put(getConfigKey(TieredStorageConfiguration.METADATA_FIELD_BUCKET), bucket);
        metaData.put(getConfigKey(TieredStorageConfiguration.METADATA_FIELD_ENDPOINT), "");

        TieredStorageConfiguration config = TieredStorageConfiguration.create(metaData);
        config.setProviderCredentials(getBlobStoreCredentials());

        return config;
    }

    private String getConfigKey(String field) {
        return TieredStorageConfiguration.OFFLOADER_PROPERTY_PREFIX + StringUtils.capitalize(field);
    }
    /*
     * Get the credentials to use for the JCloud provider
     * based on the System properties.
     */
    protected static Supplier<Credentials> getBlobStoreCredentials() {
        if (Boolean.parseBoolean(System.getProperty("testRealAWS", "false"))) {
            /* To use this, must config credentials using "aws_access_key_id" as S3ID,
             *  and "aws_secret_access_key" as S3Key. And bucket should exist in default region. e.g.
             *      props.setProperty("S3ID", "AXXXXXXQ");
             *      props.setProperty("S3Key", "HXXXXXß");
             */
            return () -> new Credentials(System.getProperty("S3ID"), System.getProperty("S3Key"));

        } else if (Boolean.parseBoolean(System.getProperty("testRealGCS", "false"))) {
            /*
             * To use this, must config credentials using "client_email" as GCSID and "private_key" as GCSKey.
             * And bucket should exist in default region. e.g.
             *        props.setProperty("GCSID", "5XXXXXXXXXX6-compute@developer.gserviceaccount.com");
             *        props.setProperty("GCSKey", "XXXXXX");
             */
            return () -> new Credentials(System.getProperty("GCSID"), System.getProperty("GCSKey"));
        } else {
            return null;
        }
    }
}
