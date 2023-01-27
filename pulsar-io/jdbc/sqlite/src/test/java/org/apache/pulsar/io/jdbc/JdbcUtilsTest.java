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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.jdbc.JdbcUtils.TableDefinition;
import org.apache.pulsar.io.jdbc.JdbcUtils.TableId;
import org.assertj.core.util.Lists;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Jdbc Utils test
 */
@Slf4j
public class JdbcUtilsTest {

    private final SqliteUtils sqliteUtils = new SqliteUtils(getClass().getSimpleName());
    @BeforeMethod
    public void setUp() throws IOException, SQLException {
        sqliteUtils.setUp();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException, SQLException {
        sqliteUtils.tearDown();
    }

    @DataProvider(name = "excludeNonDeclaredFields")
    public Object[] excludeNonDeclaredFields() {
        return new Object[]{
                false, true
        };
    }

    @Test(dataProvider = "excludeNonDeclaredFields")
    public void testGetTableId(boolean excludeNonDeclaredFields) throws Exception {
        String tableName = "TestGetTableId";

        sqliteUtils.createTable(
            "CREATE TABLE " + tableName + "(" +
                "    firstName  TEXT," +
                "    lastName  TEXT," +
                "    age INTEGER," +
                "    bool  NUMERIC NULL," +
                "    byte  INTEGER NULL," +
                "    short INTEGER NULL," +
                "    long INTEGER," +
                "    float NUMERIC NULL," +
                "    double NUMERIC NULL," +
                "    bytes BLOB NULL, " +
                "PRIMARY KEY (firstName, lastName));"
        );

        try (Connection connection = sqliteUtils.getConnection(true);) {

            // Test getTableId
            log.info("verify getTableId");
            TableId id = JdbcUtils.getTableId(connection, tableName);
            Assert.assertEquals(id.getTableName(), tableName);

            // Test get getTableDefinition
            log.info("verify getTableDefinition");
            List<String> keyList = Lists.newArrayList();
            keyList.add("firstName");
            keyList.add("lastName");
            List<String> nonKeyList = Lists.newArrayList();
            nonKeyList.add("age");
            nonKeyList.add("long");
            TableDefinition table = JdbcUtils.getTableDefinition(connection, id, keyList, nonKeyList,
                    excludeNonDeclaredFields);
            if (!excludeNonDeclaredFields) {
                Assert.assertEquals(table.getColumns().size(), 10);
                Assert.assertEquals(table.getColumns().get(0).getName(), "firstName");
                Assert.assertEquals(table.getColumns().get(0).getTypeName(), "TEXT");
                Assert.assertEquals(table.getColumns().get(2).getName(), "age");
                Assert.assertEquals(table.getColumns().get(2).getTypeName(), "INTEGER");
                Assert.assertEquals(table.getColumns().get(7).getName(), "float");
                Assert.assertEquals(table.getColumns().get(7).getTypeName(), "NUMERIC");

                Assert.assertEquals(table.getKeyColumns().size(), 2);
                Assert.assertEquals(table.getKeyColumns().get(0).getName(), "firstName");
                Assert.assertEquals(table.getKeyColumns().get(0).getTypeName(), "TEXT");
                Assert.assertEquals(table.getKeyColumns().get(1).getName(), "lastName");
                Assert.assertEquals(table.getKeyColumns().get(1).getTypeName(), "TEXT");

                Assert.assertEquals(table.getNonKeyColumns().size(), 2);
                Assert.assertEquals(table.getNonKeyColumns().get(0).getName(), "age");
                Assert.assertEquals(table.getNonKeyColumns().get(0).getTypeName(), "INTEGER");
                Assert.assertEquals(table.getNonKeyColumns().get(1).getName(), "long");
                Assert.assertEquals(table.getNonKeyColumns().get(1).getTypeName(), "INTEGER");
                // Test get getTableDefinition
                log.info("verify buildInsertSql");
                String expctedInsertStatement = "INSERT INTO " + tableName +
                        "(firstName,lastName,age,bool,byte,short,long,float,double,bytes)" +
                        " VALUES(?,?,?,?,?,?,?,?,?,?)";
                String insertStatement = JdbcUtils.buildInsertSql(table);
                Assert.assertEquals(insertStatement, expctedInsertStatement);
                log.info("verify buildUpdateSql");
                String expectedUpdateStatement = "UPDATE " + tableName +
                        " SET age=? ,long=?  WHERE firstName=? AND lastName=?";
                String updateStatement = JdbcUtils.buildUpdateSql(table);
                Assert.assertEquals(updateStatement, expectedUpdateStatement);
                log.info("verify buildDeleteSql");
                String expectedDeleteStatement = "DELETE FROM " + tableName +
                        " WHERE firstName=? AND lastName=?";
                String deleteStatement = JdbcUtils.buildDeleteSql(table);
                Assert.assertEquals(deleteStatement, expectedDeleteStatement);
            } else {
                Assert.assertEquals(table.getColumns().size(), 4);
                Assert.assertEquals(table.getColumns().get(0).getName(), "firstName");
                Assert.assertEquals(table.getColumns().get(0).getTypeName(), "TEXT");
                Assert.assertEquals(table.getColumns().get(1).getName(), "lastName");
                Assert.assertEquals(table.getColumns().get(1).getTypeName(), "TEXT");
                Assert.assertEquals(table.getColumns().get(2).getName(), "age");
                Assert.assertEquals(table.getColumns().get(2).getTypeName(), "INTEGER");
                Assert.assertEquals(table.getColumns().get(3).getName(), "long");
                Assert.assertEquals(table.getColumns().get(3).getTypeName(), "INTEGER");


                Assert.assertEquals(table.getKeyColumns().size(), 2);
                Assert.assertEquals(table.getKeyColumns().get(0).getName(), "firstName");
                Assert.assertEquals(table.getKeyColumns().get(0).getTypeName(), "TEXT");
                Assert.assertEquals(table.getKeyColumns().get(1).getName(), "lastName");
                Assert.assertEquals(table.getKeyColumns().get(1).getTypeName(), "TEXT");

                Assert.assertEquals(table.getNonKeyColumns().size(), 2);
                Assert.assertEquals(table.getNonKeyColumns().get(0).getName(), "age");
                Assert.assertEquals(table.getNonKeyColumns().get(0).getTypeName(), "INTEGER");
                Assert.assertEquals(table.getNonKeyColumns().get(1).getName(), "long");
                Assert.assertEquals(table.getNonKeyColumns().get(1).getTypeName(), "INTEGER");
                // Test get getTableDefinition
                log.info("verify buildInsertSql");
                String expctedInsertStatement = "INSERT INTO " + tableName +
                        "(firstName,lastName,age,long)" +
                        " VALUES(?,?,?,?)";
                String insertStatement = JdbcUtils.buildInsertSql(table);
                Assert.assertEquals(insertStatement, expctedInsertStatement);
                log.info("verify buildUpdateSql");
                String expectedUpdateStatement = "UPDATE " + tableName +
                        " SET age=? ,long=?  WHERE firstName=? AND lastName=?";
                String updateStatement = JdbcUtils.buildUpdateSql(table);
                Assert.assertEquals(updateStatement, expectedUpdateStatement);
                log.info("verify buildDeleteSql");
                String expectedDeleteStatement = "DELETE FROM " + tableName +
                        " WHERE firstName=? AND lastName=?";
                String deleteStatement = JdbcUtils.buildDeleteSql(table);
                Assert.assertEquals(deleteStatement, expectedDeleteStatement);

            }
        }
    }
}
