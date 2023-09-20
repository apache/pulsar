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
package org.apache.pulsar.broker.authentication.oidc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

public class ConfigUtilsTest {

    @Test
    public void testGetConfigValueAsStringWorks() {
        Properties props = new Properties();
        props.setProperty("prop1", "audience");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        String actual = ConfigUtils.getConfigValueAsString(config, "prop1");
        assertEquals("audience", actual);
    }

    @Test
    public void testGetConfigValueAsStringReturnsNullIfMissing() {
        Properties props = new Properties();
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        String actual = ConfigUtils.getConfigValueAsString(config, "prop1");
        assertNull(actual);
    }

    @Test
    public void testGetConfigValueAsStringWithDefaultWorks() {
        Properties props = new Properties();
        props.setProperty("prop1", "audience");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        String actual = ConfigUtils.getConfigValueAsString(config, "prop1", "default");
        assertEquals("audience", actual);
    }

    @Test
    public void testGetConfigValueAsStringReturnsDefaultIfMissing() {
        Properties props = new Properties();
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        String actual = ConfigUtils.getConfigValueAsString(config, "prop1", "default");
        assertEquals("default", actual);
    }

    @Test
    public void testGetConfigValueAsSetReturnsWorks() {
        Properties props = new Properties();
        props.setProperty("prop1", "a, b,   c");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        Set<String> actual = ConfigUtils.getConfigValueAsSet(config, "prop1");
        // Trims all whitespace
        assertEquals(Set.of("a", "b", "c"), actual);
    }

    @Test
    public void testGetConfigValueAsSetReturnsEmptySetIfMissing() {
        Properties props = new Properties();
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        Set<String> actual = ConfigUtils.getConfigValueAsSet(config, "prop1");
        assertEquals(Collections.emptySet(), actual);
    }

    @Test
    public void testGetConfigValueAsSetReturnsEmptySetIfBlankString() {
        Properties props = new Properties();
        props.setProperty("prop1", " ");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        Set<String> actual = ConfigUtils.getConfigValueAsSet(config, "prop1");
        assertEquals(Collections.emptySet(), actual);
    }

    @Test
    public void testGetConfigValueAsIntegerWorks() {
        Properties props = new Properties();
        props.setProperty("prop1", "1234");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        int actual = ConfigUtils.getConfigValueAsInt(config, "prop1", 9);
        assertEquals(1234, actual);
    }

    @Test
    public void testGetConfigValueAsIntegerReturnsDefaultIfNAN() {
        Properties props = new Properties();
        props.setProperty("prop1", "non-a-number");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        int actual = ConfigUtils.getConfigValueAsInt(config, "prop1", 9);
        assertEquals(9, actual);
    }

    @Test
    public void testGetConfigValueAsIntegerReturnsDefaultIfMissingProp() {
        Properties props = new Properties();
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        int actual = ConfigUtils.getConfigValueAsInt(config, "prop1", 10);
        assertEquals(10, actual);
    }

    @Test
    public void testGetConfigValueAsBooleanReturnsDefaultIfMissingProp() {
        Properties props = new Properties();
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        boolean actualFalse = ConfigUtils.getConfigValueAsBoolean(config, "prop1", false);
        assertFalse(actualFalse);
        boolean actualTrue = ConfigUtils.getConfigValueAsBoolean(config, "prop1", true);
        assertTrue(actualTrue);
    }

    @Test
    public void testGetConfigValueAsBooleanWorks() {
        Properties props = new Properties();
        props.setProperty("prop1", "true");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setProperties(props);
        boolean actual = ConfigUtils.getConfigValueAsBoolean(config, "prop1", false);
        assertTrue(actual);
    }

}
