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

import static org.junit.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class FileSourceConfigTests {

    private final static String INPUT_DIRECTORY = "/dev/null";

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        FileSourceConfig config = FileSourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
    }
    
    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", INPUT_DIRECTORY);
        map.put("keepFile", false);
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
    }
    
    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", INPUT_DIRECTORY);
        
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
        map.put("inputDirectory", INPUT_DIRECTORY);
        map.put("recurse", "not a boolean");
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "The property pollingInterval must be greater than zero")
    public final void ZeroValueTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", INPUT_DIRECTORY);
        map.put("pollingInterval", 0);
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "The property minimumFileAge must be non-negative")
    public final void NegativeValueTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", INPUT_DIRECTORY);
        map.put("minimumFileAge", "-50");
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Invalid Regex pattern provided for fileFilter")
    public final void invalidFileFilterTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", INPUT_DIRECTORY);
        map.put("fileFilter", "\\");  // Results in a single '\' being sent.
        
        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        config.validate();
    }

    @Test
    public final void keepFileTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", "/"); // root directory that we cannot write to
        map.put("keepFile", "true"); // even though no write permission on "/", we should still be able to read

        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        assertTrue(config.getKeepFile());
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "You have requested the consumed files to be deleted, " +
                    "but the source directory is not writeable.")
    public final void invalidKeepFileTest() throws  IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("inputDirectory", "/"); // root directory that we cannot write to
        map.put("keepFile", "false");

        FileSourceConfig config = FileSourceConfig.load(map);
        assertNotNull(config);
        assertFalse(config.getKeepFile());
        config.validate();
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
