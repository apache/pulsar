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

package org.apache.pulsar.io.jdbc;


import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public final class SqliteUtils {

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            throw new RuntimeException(e);
        }
    }

    public interface ResultSetReadCallback {
        void read(final ResultSet rs) throws SQLException;
    }

    private final Path dbPath;

    private Connection connection;

    public Connection getConnection() {
        return connection;
    }

    public SqliteUtils(String testId) {
        dbPath = Paths.get(testId + ".db");
    }

    public String sqliteUri() {
        return "jdbc:sqlite:" + dbPath;
    }

    public void setUp() throws SQLException, IOException {
        Files.deleteIfExists(dbPath);
        connection = DriverManager.getConnection(sqliteUri());
        connection.setAutoCommit(false);
    }

    public void tearDown() throws SQLException, IOException {
        connection.close();
        Files.deleteIfExists(dbPath);
    }

    public void createTable(final String createSql) throws SQLException {
        execute(createSql);
    }

    public void deleteTable(final String table) throws SQLException {
        execute("DROP TABLE IF EXISTS " + table);

        //random errors of table not being available happens in the unit tests
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public int select(final String query, final ResultSetReadCallback callback) throws SQLException {
        int count = 0;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    callback.read(rs);
                    count++;
                }
            }
        }
        return count;
    }

    public void execute(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            connection.commit();
        }
    }

}
