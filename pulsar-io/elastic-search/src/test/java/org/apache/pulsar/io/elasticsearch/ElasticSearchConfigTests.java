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
package org.apache.pulsar.io.elasticsearch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class ElasticSearchConfigTests {

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
    }
    
    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myIndex");
        map.put("typeName", "doc");
        map.put("username", "racerX");
        map.put("password", "go-speedie-go");
        
        ElasticSearchConfig config = ElasticSearchConfig.load(map);
        assertNotNull(config);
        assertEquals(config.getElasticSearchUrl(), "http://localhost:90902");
        assertEquals(config.getIndexName(), "myIndex");
        assertEquals(config.getTypeName(), "doc");
        assertEquals(config.getUsername(), "racerX");
        assertEquals(config.getPassword(), "go-speedie-go");  
    }

    @Test
    public final void defaultValueTest() throws IOException {
        ElasticSearchConfig config = ElasticSearchConfig.load(Collections.emptyMap());

        assertNull(config.getElasticSearchUrl());
        assertNull(config.getIndexName());
        assertEquals(config.getTypeName(), "_doc");
        assertNull(config.getUsername());
        assertNull(config.getPassword());
        assertEquals(config.getIndexNumberOfReplicas(), 1);
        assertEquals(config.getIndexNumberOfShards(), 1);
    }
    
    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myIndex");
        map.put("username", "racerX");
        map.put("password", "go-speedie-go");
        
        ElasticSearchConfig config = ElasticSearchConfig.load(map);
        assertNotNull(config);
        config.validate();
    }
    
    @Test
    public final void zeroReplicasValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myIndex");
        map.put("username", "racerX");
        map.put("password", "go-speedie-go");
        map.put("indexNumberOfReplicas", "0");
        
        ElasticSearchConfig config = ElasticSearchConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Required property not set.")
    public final void missingRequiredPropertiesTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        
        ElasticSearchConfig config = ElasticSearchConfig.load(map);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "indexNumberOfShards must be a strictly positive integer")
    public final void invalidPropertyValueTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myIndex");
        map.put("username", "racerX");
        map.put("password", "go-speedie-go");
        map.put("indexNumberOfShards", "-1");
        
        ElasticSearchConfig config = ElasticSearchConfig.load(map);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Values for both Username & password are required.")
    public final void userCredentialsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myIndex");
        map.put("username", "racerX");
       
        ElasticSearchConfig config = ElasticSearchConfig.load(map);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Values for both Username & password are required.")
    public final void passwordCredentialsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:90902");
        map.put("indexName", "myIndex");
        map.put("password", "go-speedie-go");
       
        ElasticSearchConfig config = ElasticSearchConfig.load(map);
        config.validate();
    }
    
    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
