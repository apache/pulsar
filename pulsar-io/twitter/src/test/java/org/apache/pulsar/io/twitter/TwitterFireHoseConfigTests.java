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
package org.apache.pulsar.io.twitter;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

public class TwitterFireHoseConfigTests {
    
    private TwitterFireHoseConfig config;

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sourceConfig.yaml");
        config = TwitterFireHoseConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("consumerKey", "xxx");
        map.put("consumerSecret", "xxx");
        map.put("token", "xxx");
        map.put("tokenSecret", "xxx");
        
        config = TwitterFireHoseConfig.load(map);
        
        assertNotNull(config);
    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("consumerKey", "xxx");
        map.put("consumerSecret", "xxx");
        map.put("token", "xxx");
        map.put("tokenSecret", "xxx");
        
        config = TwitterFireHoseConfig.load(map);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Required property not set.")
    public final void missingConsumerKeyValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        
        config = TwitterFireHoseConfig.load(map);
        config.validate();
    }
    
    @Test
    public final void getFollowingsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("followings", "123, 456, 789");
        config = TwitterFireHoseConfig.load(map);
        
        List<Long> followings = config.getFollowings();
        assertNotNull(followings);
        assertEquals(followings.size(), 3);
        assertTrue(followings.contains(123L));
        assertTrue(followings.contains(456L));
        assertTrue(followings.contains(789L));
    }
    
    
    @Test
    public final void getTermsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("terms", "mickey, donald, goofy");
        config = TwitterFireHoseConfig.load(map);
        
        List<String> terms = config.getTrackTerms();
        assertNotNull(terms);
        assertEquals(terms.size(), 3);
        assertTrue(terms.contains("mickey"));
    }
    
    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }

}
