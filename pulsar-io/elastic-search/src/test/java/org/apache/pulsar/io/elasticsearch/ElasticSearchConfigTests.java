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
package org.apache.pulsar.io.elasticsearch;

import org.apache.pulsar.io.core.SinkContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;


public class ElasticSearchConfigTests {

    private final SinkContext mockContext = Mockito.mock(SinkContext.class);

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        ElasticSearchConfig config = ElasticSearchConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
        assertEquals(config.getElasticSearchUrl(), "http://localhost:90902");
        assertEquals(config.getIndexName(), "myIndex");
        assertEquals(config.getTypeName(), "doc");
        assertEquals(config.getUsername(), "scooby");
        assertEquals(config.getPassword(), "doobie");
        assertEquals(config.getPrimaryFields(), "id,a");
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myIndex");
        map.put("typeName", "doc");
        map.put("username", "racerX");
        map.put("password", "go-speedie-go");
        map.put("primaryFields", "x");

        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        assertNotNull(config);
        assertEquals(config.getElasticSearchUrl(), "http://localhost:90902");
        assertEquals(config.getIndexName(), "myIndex");
        assertEquals(config.getTypeName(), "doc");
        assertEquals(config.getUsername(), "racerX");
        assertEquals(config.getPassword(), "go-speedie-go");
        assertEquals(config.getPrimaryFields(), "x");
    }

    @Test
    public final void defaultValueTest() throws IOException {
        Map<String, Object> requiredConfig = Map.of("elasticSearchUrl", "http://localhost:90902");
        ElasticSearchConfig config = ElasticSearchConfig.load(requiredConfig, mockContext);
        assertNull(config.getIndexName());
        assertEquals(config.getTypeName(), "_doc");
        assertNull(config.getUsername());
        assertNull(config.getPassword());
        assertNull(config.getToken());
        assertNull(config.getApiKey());
        assertEquals(config.getIndexNumberOfReplicas(), 0);
        assertEquals(config.getIndexNumberOfShards(), 1);

        assertEquals(config.isBulkEnabled(), false);
        assertEquals(config.getBulkActions(), 1000L);
        assertEquals(config.getBulkSizeInMb(), 5L);
        assertEquals(config.getBulkFlushIntervalInMs(), 1000L);
        assertEquals(config.getBulkConcurrentRequests(), 0L);

        assertEquals(config.isCompressionEnabled(), false);
        assertEquals(config.getConnectTimeoutInMs(), 5000L);
        assertEquals(config.getConnectionRequestTimeoutInMs(), 1000L);
        assertEquals(config.getConnectionIdleTimeoutInMs(), 5L);
        assertEquals(config.getSocketTimeoutInMs(), 60000);

        assertEquals(config.isStripNulls(), true);
        assertEquals(config.isCopyKeyFields(), false);
        assertEquals(config.isSchemaEnable(), false);
        assertEquals(config.isKeyIgnore(), true);
        assertEquals(config.getMalformedDocAction(), ElasticSearchConfig.MalformedDocAction.FAIL);
        assertEquals(config.getNullValueAction(), ElasticSearchConfig.NullValueAction.IGNORE);

        assertEquals(config.getMaxRetries(), 1);
        assertEquals(config.getMaxRetryTimeInSec(), 86400L);
        assertEquals(config.getRetryBackoffInMs(), 100L);

        assertEquals(config.getSsl().isEnabled(), false);
        assertNull(config.getSsl().getProvider());
        assertNull(config.getSsl().getCipherSuites());
        assertEquals(config.getSsl().isHostnameVerification(), true);
        assertEquals(config.getSsl().getProtocols(), "TLSv1.2");

        assertEquals(config.getCompatibilityMode(), ElasticSearchConfig.CompatibilityMode.AUTO);
        assertEquals(config.getIdHashingAlgorithm(), ElasticSearchConfig.IdHashingAlgorithm.NONE);
    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myindex");
        map.put("username", "racerX");
        map.put("password", "go-speedie-go");

        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        assertNotNull(config);
        config.validate();
    }

    @Test
    public final void zeroReplicasValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myindex");
        map.put("username", "racerX");
        map.put("password", "go-speedie-go");
        map.put("indexNumberOfReplicas", "0");

        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "elasticSearchUrl cannot be null")
    public final void missingRequiredPropertiesTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("indexName", "toto");

        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "indexNumberOfShards must be a strictly positive integer.")
    public final void invalidIndexNumberOfShards() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myindex");
        map.put("username", "racerX");
        map.put("password", "go-speedie-go");
        map.put("indexNumberOfShards", "0");

        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "indexNumberOfReplicas must be a positive integer.")
    public final void invalidIndexNumberOfReplicas() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myindex");
        map.put("username", "racerX");
        map.put("password", "go-speedie-go");
        map.put("indexNumberOfReplicas", "-1");

        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Values for both Username & password are required.")
    public final void userCredentialsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myindex");
        map.put("username", "racerX");

        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Values for both Username & password are required.")
    public final void passwordCredentialsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myindex");
        map.put("password", "go-speedie-go");

        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
    }

    @Test
    public final void credentialsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myindex");
        map.put("username", "elastic");
        map.put("password", "go-speedie-go");
        map.put("token", "tok");
        {
            ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
            expectThrows(IllegalArgumentException.class, () -> config.validate());
        }
        map.put("apiKey", "apiKey");
        {
            ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
            expectThrows(IllegalArgumentException.class, () -> config.validate());
        }
        map.remove("token");
        {
            ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
            expectThrows(IllegalArgumentException.class, () -> config.validate());
        }
        map.remove("username");
        map.remove("password");
        {
            ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
            config.validate();
        }
        map.put("token", "tok");
        map.remove("apiKey");
        {
            ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
            config.validate();
        }
        map.remove("token");

        {
            ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
            config.validate();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "connectTimeoutInMs must be a positive integer.")
    public final void connectTimeoutInMsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("connectTimeoutInMs", -1);
        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "connectionRequestTimeoutInMs must be a positive integer.")
    public final void connectionRequestTimeoutInMsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("connectionRequestTimeoutInMs", -1);
        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "socketTimeoutInMs must be a positive integer.")
    public final void socketTimeoutInMsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("socketTimeoutInMs", -1);
        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "bulkConcurrentRequests must be a positive integer.")
    public final void bulkConcurrentRequestsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("bulkConcurrentRequests", -1);
        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }

    @Test
    public final void sslConfigTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myindex");

        Map<String, Object> sslMap = new HashMap<String, Object> ();
        sslMap.put("enabled", true);
        sslMap.put("truststorePath", "/ssl/truststore.jks");
        sslMap.put("truststorePassword", "toto");
        sslMap.put("keystorePath", "/ssl/keystore.jks");
        sslMap.put("keystorePassword", "titi");
        sslMap.put("hostnameVerification", false);
        sslMap.put("protocols", "TLSv1.2,TLSv1.3");
        sslMap.put("provider", "Sun");
        map.put("ssl", sslMap);

        ElasticSearchConfig config = ElasticSearchConfig.load(map, mockContext);
        config.validate();
        assertEquals(config.getSsl().isEnabled(), true);
        assertEquals(config.getSsl().getTruststorePath(), "/ssl/truststore.jks");
        assertEquals(config.getSsl().getTruststorePassword(), "toto");
        assertEquals(config.getSsl().getKeystorePath(), "/ssl/keystore.jks");
        assertEquals(config.getSsl().getKeystorePassword(), "titi");
        assertEquals(config.getSsl().isHostnameVerification(), false);
        assertEquals(config.getSsl().getProtocols(), "TLSv1.2,TLSv1.3");
        assertEquals(config.getSsl().getProvider(), "Sun");
    }

    @Test
    public final void loadConfigFromSecretsTest() throws IOException {
        SinkContext contextWithSecrets = Mockito.mock(SinkContext.class);
        Mockito.when(contextWithSecrets.getSecret("username")).thenReturn("secretUser");
        Mockito.when(contextWithSecrets.getSecret("password")).thenReturn("$ecret123");

        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myIndex");
        map.put("typeName", "doc");
        map.put("primaryFields", "x");

        ElasticSearchConfig config = ElasticSearchConfig.load(map, contextWithSecrets);
        assertNotNull(config);
        assertEquals(config.getElasticSearchUrl(), "http://localhost:90902");
        assertEquals(config.getIndexName(), "myIndex");
        assertEquals(config.getTypeName(), "doc");
        assertEquals(config.getPrimaryFields(), "x");
        assertEquals(config.getUsername(), "secretUser");
        assertEquals(config.getPassword(), "$ecret123");
    }
}