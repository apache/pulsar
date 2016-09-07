/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.common.naming;

import static com.yahoo.pulsar.broker.ServiceConfigurationLoader.isComplete;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.InputStream;
import java.util.Properties;

import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.FieldContext;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.ServiceConfigurationLoader;

public class ServiceConfigurationLoaderTest {

    @Test
    public void testServiceConfiguraitonLoadingStream() throws Exception {
        final String fileName = "configurations/pulsar_broker_test.conf"; // test-resource file
        InputStream stream = this.getClass().getClassLoader().getResourceAsStream(fileName);
        final ServiceConfiguration serviceConfig = ServiceConfigurationLoader.create(stream);
        assertNotNull(serviceConfig);
    }

    @Test
    public void testServiceConfiguraitonLoadingProp() throws Exception {
        final String zk = "localhost:2184";
        final Properties prop = new Properties();
        prop.setProperty("zookeeperServers", zk);
        final ServiceConfiguration serviceConfig = ServiceConfigurationLoader.create(prop);
        assertNotNull(serviceConfig);
        assertEquals(serviceConfig.getZookeeperServers(), zk);
    }

    @Test
    public void testServiceConfiguraitonComplete() throws Exception {
        final String zk = "localhost:2184";
        final Properties prop = new Properties();
        prop.setProperty("zookeeperServers", zk);
        final ServiceConfiguration serviceConfig = ServiceConfigurationLoader.create(prop);
        assertEquals(serviceConfig.getZookeeperServers(), zk);
    }

    @Test
    public void testComplete() throws Exception {
        TestCompleteObject complete = this.new TestCompleteObject();
        assertTrue(isComplete(complete));
    }

    @Test
    public void testInComplete() throws IllegalAccessException {

        try {
            isComplete(this.new TestInCompleteObjectRequired());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            isComplete(this.new TestInCompleteObjectMin());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            isComplete(this.new TestInCompleteObjectMax());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            isComplete(this.new TestInCompleteObjectMix());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }
    }

    class TestCompleteObject {
        @FieldContext(required = true)
        String required = "I am not null";
        @FieldContext(required = false)
        String optional;
        @FieldContext
        String optional2;
        @FieldContext(minValue = 1)
        int minValue = 2;
        @FieldContext(minValue = 1, maxValue = 3)
        int minMaxValue = 2;

    }

    class TestInCompleteObjectRequired {
        @FieldContext(required = true)
        String inValidRequired;
    }

    class TestInCompleteObjectMin {
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMin = 0;
    }

    class TestInCompleteObjectMax {
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMax = 4;
    }

    class TestInCompleteObjectMix {
        @FieldContext(required = true)
        String inValidRequired;
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMin = 0;
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMax = 4;
    }
}
