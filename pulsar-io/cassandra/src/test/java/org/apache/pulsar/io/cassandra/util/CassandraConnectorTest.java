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
package org.apache.pulsar.io.cassandra.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import org.apache.pulsar.io.cassandra.CassandraSinkConfig;
import org.testng.annotations.Test;

public class CassandraConnectorTest {

    private CassandraSinkConfig config;

    @Test(enabled = false)
    public final void securedTest() {
        config = new CassandraSinkConfig();
        config.setRoots("localhost");
        config.setUserName("cassandra");
        config.setPassword("cassandra");

        CassandraConnector connector = new CassandraConnector(config);
        connector.connect();
        assertNotNull(connector.getSession());
    }

    @Test(enabled = false)
    public final void getObservationPreparedStatementTest() {
        config = new CassandraSinkConfig();
        config.setRoots("localhost");
        config.setUserName("cassandra");
        config.setPassword("cassandra");
        config.setColumnFamily("observation");
        config.setKeyspace("airquality");

        CassandraConnector connector = new CassandraConnector(config);
        assertEquals("INSERT INTO airquality.observation (key, observed) VALUES (?, ?)",
                connector.getPreparedStatement().getQueryString());
    }

    @Test(enabled = false)
    public final void getReadingPreparedStatementTest() {
        config = new CassandraSinkConfig();
        config.setRoots("localhost");
        config.setUserName("cassandra");
        config.setPassword("cassandra");
        config.setColumnFamily("reading");
        config.setKeyspace("airquality");

        CassandraConnector connector = new CassandraConnector(config);
        assertEquals("INSERT INTO airquality.reading "
                        + "(reporting_area, avg_ozone, avg_pm10, avg_pm25, date_observed, hour_observed, latitude, "
                        + "local_time_zone, longitude, max_ozone, max_pm10, max_pm25, min_ozone, min_pm10, min_pm25, "
                        + "readingid, state_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                connector.getPreparedStatement().getQueryString());
    }
}
