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
package org.apache.bookkeeper.mledger.offload.jcloud.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.awaitility.Awaitility;
import org.jclouds.Constants;
import org.jclouds.aws.domain.SessionCredentials;
import org.jclouds.domain.Credentials;
import org.mockito.MockedConstruction;
import org.testng.annotations.Test;

public class TieredStorageConfigurationTest {

    @Test
    public void testStorageCheckUsesIndependentConfiguration() {
        TieredStorageConfiguration config = new TieredStorageConfiguration(Map.of(
                TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, "aws-s3",
                "managedLedgerOffloadBucket", "offload-bucket",
                "managedLedgerOffloadRegion", "us-east-1"));
        Supplier<Credentials> credentials = () -> new Credentials("identity", "credential");
        config.setProviderCredentials(credentials);
        Properties originalOverrides = config.getOverrides();

        TieredStorageConfiguration checkConfig = config.forStorageCheck(Duration.ofSeconds(2));

        assertNotSame(checkConfig.getConfigProperties(), config.getConfigProperties());
        assertEquals(checkConfig.getConfigProperties(), config.getConfigProperties());
        assertSame(checkConfig.getProvider(), config.getProvider());
        assertSame(checkConfig.getProviderCredentials(), credentials);
        Properties overrides = checkConfig.getOverrides();
        assertEquals(overrides.getProperty(Constants.PROPERTY_MAX_RETRIES), "0");
        assertEquals(overrides.getProperty(Constants.PROPERTY_CONNECTION_TIMEOUT), "2000");
        assertEquals(overrides.getProperty(Constants.PROPERTY_SO_TIMEOUT), "2000");
        assertEquals(overrides.getProperty(Constants.PROPERTY_REQUEST_TIMEOUT), "2000");
        assertEquals(config.getOverrides(), originalOverrides);
    }

    @Test
    public void testStorageCheckSharesLazilyInitializedAssumeRoleCredentials() {
        TieredStorageConfiguration config = new TieredStorageConfiguration(Map.of(
                TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, "aws-s3",
                TieredStorageConfiguration.S3_ROLE_FIELD, "arn:aws:iam::123456789012:role/offloader",
                TieredStorageConfiguration.S3_ROLE_SESSION_NAME_FIELD, "storage-check"));
        STSAssumeRoleSessionCredentialsProvider roleProvider = mock(STSAssumeRoleSessionCredentialsProvider.class);
        when(roleProvider.getCredentials()).thenReturn(
                new BasicSessionCredentials("identity", "credential", "first-token"),
                new BasicSessionCredentials("identity", "credential", "refreshed-token"));
        try (MockedConstruction<STSAssumeRoleSessionCredentialsProvider.Builder> builders = mockConstruction(
                STSAssumeRoleSessionCredentialsProvider.Builder.class,
                (builder, context) -> when(builder.build()).thenReturn(roleProvider))) {
            assertThat(config.getCredentials()).isNull();

            TieredStorageConfiguration checkConfig = config.forStorageCheck(Duration.ofSeconds(2));
            Supplier<Credentials> checkCredentials = checkConfig.getProviderCredentials();
            Supplier<Credentials> offloadCredentials = config.getProviderCredentials();

            assertThat(checkCredentials).isSameAs(offloadCredentials);
            assertThat(builders.constructed()).hasSize(1);
            verify(roleProvider, never()).getCredentials();
            // Share the provider, not a snapshot of its expiring session credentials.
            assertThat(checkCredentials.get()).isInstanceOfSatisfying(SessionCredentials.class,
                    credentials -> assertThat(credentials.getSessionToken()).isEqualTo("first-token"));
            assertThat(offloadCredentials.get()).isInstanceOfSatisfying(SessionCredentials.class,
                    credentials -> assertThat(credentials.getSessionToken()).isEqualTo("refreshed-token"));
        }
    }

    @Test(timeOut = 30000)
    public void testStorageCheckSharesCredentialsDuringConcurrentInitialization() throws Exception {
        AtomicInteger initializations = new AtomicInteger();
        CountDownLatch initializationStarted = new CountDownLatch(1);
        CountDownLatch releaseInitialization = new CountDownLatch(1);
        TieredStorageConfiguration config = new TieredStorageConfiguration(Map.of(
                TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, "aws-s3",
                TieredStorageConfiguration.S3_ID_FIELD, "identity",
                TieredStorageConfiguration.S3_SECRET_FIELD, "credential")) {
            @Override
            public void setProviderCredentials(Supplier<Credentials> credentials) {
                initializations.incrementAndGet();
                initializationStarted.countDown();
                try {
                    assertThat(releaseInitialization.await(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                super.setProviderCredentials(credentials);
            }
        };
        FutureTask<Supplier<Credentials>> offloadCredentials = new FutureTask<>(config::getProviderCredentials);
        FutureTask<Supplier<Credentials>> checkCredentials = new FutureTask<>(
                () -> config.forStorageCheck(Duration.ofSeconds(2)).getProviderCredentials());
        Thread offloadThread = new Thread(offloadCredentials, "offload-credentials");
        Thread checkThread = new Thread(checkCredentials, "storage-check-credentials");
        try {
            offloadThread.start();
            assertThat(initializationStarted.await(10, TimeUnit.SECONDS)).isTrue();
            checkThread.start();
            // Hold the first provider before publication until the check either waits for it or creates another.
            Awaitility.await().until(() -> {
                ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(checkThread.threadId());
                return checkCredentials.isDone() || initializations.get() > 1
                        || (info != null && info.getThreadState() == Thread.State.BLOCKED
                        && info.getLockOwnerId() == offloadThread.threadId());
            });
            assertThat(initializations.get()).isEqualTo(1);
        } finally {
            releaseInitialization.countDown();
            offloadThread.join(10000);
            checkThread.join(10000);
        }
        assertThat(checkCredentials.get(10, TimeUnit.SECONDS))
                .isSameAs(offloadCredentials.get(10, TimeUnit.SECONDS));
        assertThat(initializations.get()).isEqualTo(1);
    }

    /*
     * Previous property names, for backwards-compatibility.
     */
    static final String BC_S3_REGION = "s3ManagedLedgerOffloadRegion";
    static final String BC_S3_BUCKET = "s3ManagedLedgerOffloadBucket";
    static final String BC_S3_ENDPOINT = "s3ManagedLedgerOffloadServiceEndpoint";
    static final String BC_S3_MAX_BLOCK_SIZE = "s3ManagedLedgerOffloadMaxBlockSizeInBytes";
    static final String BC_S3_READ_BUFFER_SIZE = "s3ManagedLedgerOffloadReadBufferSizeInBytes";

    static final String BC_GCS_BUCKET = "gcsManagedLedgerOffloadBucket";
    static final String BC_GCS_REGION = "gcsManagedLedgerOffloadRegion";
    static final String BC_GCS_MAX_BLOCK_SIZE = "gcsManagedLedgerOffloadMaxBlockSizeInBytes";
    static final String BC_GCS_READ_BUFFER_SIZE = "gcsManagedLedgerOffloadReadBufferSizeInBytes";


    /**
     * Confirm that both property options are available for AWS.
     */
    @Test
    public final void awsS3KeysTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);
        List<String> keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_BUCKET);
        assertEquals(keys.get(0), BC_S3_BUCKET);
        assertEquals(keys.get(1), "managedLedgerOffloadBucket");

        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_REGION);
        assertEquals(keys.get(0), BC_S3_REGION);
        assertEquals(keys.get(1), "managedLedgerOffloadRegion");

        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_ENDPOINT);
        assertEquals(keys.get(0), BC_S3_ENDPOINT);
        assertEquals(keys.get(1), "managedLedgerOffloadServiceEndpoint");

        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE);
        assertEquals(keys.get(0), BC_S3_MAX_BLOCK_SIZE);
        assertEquals(keys.get(1), "managedLedgerOffloadMaxBlockSizeInBytes");

        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_READ_BUFFER_SIZE);
        assertEquals(keys.get(0), BC_S3_READ_BUFFER_SIZE);
        assertEquals(keys.get(1), "managedLedgerOffloadReadBufferSizeInBytes");
    }

    /**
     * Confirm that we can configure AWS using the new properties.
     */
    @Test
    public final void awsS3PropertiesTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        map.put("managedLedgerOffloadRegion", "us-east-1");
        map.put("managedLedgerOffloadBucket", "test bucket");
        map.put("managedLedgerOffloadMaxBlockSizeInBytes", "1");
        map.put("managedLedgerOffloadReadBufferSizeInBytes", "500");
        map.put("managedLedgerOffloadServiceEndpoint", "http://some-url:9093");
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);

        assertEquals(config.getRegion(), "us-east-1");
        assertEquals(config.getBucket(), "test bucket");
        assertEquals(config.getMaxBlockSizeInBytes(), Integer.valueOf(1));
        assertEquals(config.getReadBufferSizeInBytes(), Integer.valueOf(500));
        assertEquals(config.getServiceEndpoint(), "http://some-url:9093");
    }

    /**
     * Confirm that we can configure AWS using the old properties.
     */
    @Test
    public final void awsS3BackwardCompatiblePropertiesTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        map.put(BC_S3_BUCKET, "test bucket");
        map.put(BC_S3_ENDPOINT, "http://some-url:9093");
        map.put(BC_S3_MAX_BLOCK_SIZE, "12");
        map.put(BC_S3_READ_BUFFER_SIZE, "500");
        map.put(BC_S3_REGION, "test region");
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);

        assertEquals(config.getRegion(), "test region");
        assertEquals(config.getBucket(), "test bucket");
        assertEquals(config.getMaxBlockSizeInBytes(), Integer.valueOf(12));
        assertEquals(config.getReadBufferSizeInBytes(), Integer.valueOf(500));
        assertEquals(config.getServiceEndpoint(), "http://some-url:9093");
    }

    /**
     * Confirm that with AWS we create different instances of the credentials
     * object each time we call the supplier, this ensure that we get fresh credentials
     * if the aws credential provider changes.
     */
    @Test
    public final void awsS3CredsProviderTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, JCloudBlobStoreProvider.AWS_S3.getDriver());
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);

        // set the aws properties with fake creds so the defaultProviderChain works
        System.setProperty("aws.accessKeyId", "fakeid1");
        System.setProperty("aws.secretKey", "fakekey1");
        try {
            Credentials creds1 = config.getProviderCredentials().get();
            assertEquals(creds1.identity, "fakeid1");
            assertEquals(creds1.credential, "fakekey1");

            // reset the properties and ensure we get different values by re-evaluating the chain
            System.setProperty("aws.accessKeyId", "fakeid2");
            System.setProperty("aws.secretKey", "fakekey2");
            Credentials creds2 = config.getProviderCredentials().get();
            assertEquals(creds2.identity, "fakeid2");
            assertEquals(creds2.credential, "fakekey2");
        } finally {
            System.clearProperty("aws.accessKeyId");
            System.clearProperty("aws.secretKey");
        }
    }

    /**
     * Confirm that both property options are available for GCS.
     */
    @Test
    public final void gcsKeysTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY,
                JCloudBlobStoreProvider.GOOGLE_CLOUD_STORAGE.getDriver());
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);
        List<String> keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_BUCKET);
        assertEquals(keys.get(0), BC_GCS_BUCKET);
        assertEquals(keys.get(1), "managedLedgerOffloadBucket");

        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_REGION);
        assertEquals(keys.get(0), BC_GCS_REGION);
        assertEquals(keys.get(1), "managedLedgerOffloadRegion");

        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE);
        assertEquals(keys.get(0), BC_GCS_MAX_BLOCK_SIZE);
        assertEquals(keys.get(1), "managedLedgerOffloadMaxBlockSizeInBytes");

        keys = config.getKeys(TieredStorageConfiguration.METADATA_FIELD_READ_BUFFER_SIZE);
        assertEquals(keys.get(0), BC_GCS_READ_BUFFER_SIZE);
        assertEquals(keys.get(1), "managedLedgerOffloadReadBufferSizeInBytes");
    }

    /**
     * Confirm that we can configure GCS using the new properties.
     */
    @Test
    public final void gcsPropertiesTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY,
                JCloudBlobStoreProvider.GOOGLE_CLOUD_STORAGE.getDriver());
        map.put("managedLedgerOffloadRegion", "us-east-1");
        map.put("managedLedgerOffloadBucket", "test bucket");
        map.put("managedLedgerOffloadMaxBlockSizeInBytes", "1");
        map.put("managedLedgerOffloadReadBufferSizeInBytes", "500");
        map.put("managedLedgerOffloadServiceEndpoint", "http://some-url:9093");
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);

        assertEquals(config.getRegion(), "us-east-1");
        assertEquals(config.getBucket(), "test bucket");
        assertEquals(config.getMaxBlockSizeInBytes(), Integer.valueOf(1));
        assertEquals(config.getReadBufferSizeInBytes(), Integer.valueOf(500));
    }

    /**
     * Confirm that we can configure GCS using the old properties.
     */
    @Test
    public final void gcsBackwardCompatiblePropertiesTest() {
        Map<String, String> map = new HashMap<>();
        map.put(TieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY,
                JCloudBlobStoreProvider.GOOGLE_CLOUD_STORAGE.getDriver());
        map.put(BC_GCS_BUCKET, "test bucket");
        map.put(BC_GCS_MAX_BLOCK_SIZE, "12");
        map.put(BC_GCS_READ_BUFFER_SIZE, "500");
        map.put(BC_GCS_REGION, "test region");
        TieredStorageConfiguration config = new TieredStorageConfiguration(map);

        assertEquals(config.getRegion(), "test region");
        assertEquals(config.getBucket(), "test bucket");
        assertEquals(config.getMaxBlockSizeInBytes(), Integer.valueOf(12));
        assertEquals(config.getReadBufferSizeInBytes(), Integer.valueOf(500));
    }

    @Test
    public void overridePropertiesTest() {
        Map<String, String> map = new HashMap<>();
        map.put("s3ManagedLedgerOffloadServiceEndpoint", "http://localhost");
        map.put("s3ManagedLedgerOffloadRegion", "my-region");
        System.setProperty("jclouds.SystemPropertyA", "A");
        System.setProperty("jclouds.region", "jclouds-region");
        try {
            TieredStorageConfiguration config = new TieredStorageConfiguration(map);
            Properties properties = config.getOverrides();
            assertEquals(properties.get("jclouds.region"), "jclouds-region");
            assertEquals(config.getServiceEndpoint(), "http://localhost");
            assertEquals(properties.get("jclouds.SystemPropertyA"), "A");
        } finally {
            System.clearProperty("jclouds.SystemPropertyA");
            System.clearProperty("jclouds.region");
        }
    }
}
