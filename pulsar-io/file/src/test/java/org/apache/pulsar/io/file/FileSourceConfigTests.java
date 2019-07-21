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
package org.apache.pulsar.io.file;

import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class FileSourceConfigTests {

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        FileSourceConfig config = FileSourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
    }
    
    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", "/tmp");
        map.put("keepFile", false);
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
    }
    
    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", "/tmp");
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Required property not set.")
    public final void missingRequiredPropertiesTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("pathFilter", "/");
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        config.validate();
    }
    
    @Test(expectedExceptions = com.fasterxml.jackson.databind.exc.InvalidFormatException.class)
    public final void InvalidBooleanPropertyTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", "/");
        map.put("recurse", "not a boolean");
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "The property pollingInterval must be greater than zero")
    public final void ZeroValueTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", "/");
        map.put("pollingInterval", 0);
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "The property minimumFileAge must be non-negative")
    public final void NegativeValueTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", "/");
        map.put("minimumFileAge", "-50");
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Invalid Regex pattern provided for fileFilter")
    public final void invalidFileFilterTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", "/");
        map.put("fileFilter", "\\");  // Results in a single '\' being sent.
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        config.validate();
    }
    
    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
