package org.apache.pulsar.io.cassandra.util;

import org.apache.pulsar.io.cassandra.CassandraSinkConfig;
import org.apache.pulsar.io.cassandra.util.CassandraConnector;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;

public class CassandraConnectorTest {

    private CassandraSinkConfig config;

    @Test
    @Ignore
    public final void securedTest() {
        config = new CassandraSinkConfig();
        config.setRoots("localhost");
        config.setUserName("cassandra");
        config.setPassword("cassandra");

        CassandraConnector connector = new CassandraConnector(config);
        connector.connect();
        assertNotNull(connector.getSession());
    }

    @Test
    public final void getObservationPreparedStatementTest() {
        config = new CassandraSinkConfig();
        config.setRoots("localhost");
        config.setUserName("cassandra");
        config.setPassword("cassandra");
        config.setColumnFamily("observation");
        config.setKeyspace("airquality");

        CassandraConnector connector = new CassandraConnector(config);
        assertEquals("INSERT INTO airquality.observation (key, observed) VALUES (?, ?)", connector.getPreparedStatement().getQueryString());
    }

    @Test
    public final void getReadingPreparedStatementTest() {
        config = new CassandraSinkConfig();
        config.setRoots("localhost");
        config.setUserName("cassandra");
        config.setPassword("cassandra");
        config.setColumnFamily("reading");
        config.setKeyspace("airquality");

        CassandraConnector connector = new CassandraConnector(config);
        assertEquals("INSERT INTO airquality.reading " +
                "(reporting_area, avg_ozone, avg_pm10, avg_pm25, date_observed, hour_observed, latitude, " +
                "local_time_zone, longitude, max_ozone, max_pm10, max_pm25, min_ozone, min_pm10, min_pm25, " +
                "readingid, state_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                connector.getPreparedStatement().getQueryString());
    }
}
