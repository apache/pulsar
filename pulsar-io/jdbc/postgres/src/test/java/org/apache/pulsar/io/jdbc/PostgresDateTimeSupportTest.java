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
package org.apache.pulsar.io.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PostgresDateTimeSupportTest {

    private PostgresJdbcAutoSchemaSink sink;
    private PreparedStatement mockStatement;
    private Connection mockConnection;

    @BeforeMethod
    public void setUp() throws Exception {
        sink = new PostgresJdbcAutoSchemaSink();
        mockStatement = mock(PreparedStatement.class);
        mockConnection = mock(Connection.class);

        PostgresArrayTestConfig.configureSinkWithConnection(sink, mockConnection);
    }

    @Test
    public void testTimestampHandling() throws Exception {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        boolean result = sink.handleDateTime(mockStatement, 1, timestamp, "Timestamp");

        assertTrue(result);
        verify(mockStatement).setTimestamp(1, timestamp);
    }

    @Test
    public void testDateHandling() throws Exception {
        Date date = new Date(System.currentTimeMillis());
        boolean result = sink.handleDateTime(mockStatement, 1, date, "Date");

        assertTrue(result);
        verify(mockStatement).setDate(1, date);
    }

    @Test
    public void testTimeHandling() throws Exception {
        Time time = new Time(System.currentTimeMillis());
        boolean result = sink.handleDateTime(mockStatement, 1, time, "Time");

        assertTrue(result);
        verify(mockStatement).setTime(1, time);
    }

    @Test
    public void testUnsupportedTargetType() throws Exception {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        boolean result = sink.handleDateTime(mockStatement, 1, timestamp, "UnsupportedType");

        assertFalse(result);
    }

    @Test
    public void testNullTargetType() throws Exception {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        boolean result = sink.handleDateTime(mockStatement, 1, timestamp, null);

        assertFalse(result);
    }

    @Test
    public void testEmptyTargetType() throws Exception {
        Date date = new Date(System.currentTimeMillis());
        boolean result = sink.handleDateTime(mockStatement, 1, date, "");

        assertFalse(result);
    }
}