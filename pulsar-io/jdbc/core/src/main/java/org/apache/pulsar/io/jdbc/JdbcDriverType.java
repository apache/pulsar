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

import lombok.Getter;

@Getter
public enum JdbcDriverType {

    CLICKHOUSE("jdbc:clickhouse:", "ru.yandex.clickhouse.ClickHouseDriver"),
    DB2("jdbc:db2:", "com.ibm.db2.jcc.DB2Driver"),
    DERBY_CLIENT("jdbc:derby://", "org.apache.derby.jdbc.ClientDriver"),
    DERBY_EMBEDDED("jdbc:derby:", "org.apache.derby.jdbc.EmbeddedDriver"),
    FIREBIRD("jdbc:firebird:", "org.firebirdsql.jdbc.FBDriver"),
    FIREBIRD_SQL("jdbc:firebirdsql:", "org.firebirdsql.jdbc.FBDriver"),
    H2("jdbc:h2:", "org.h2.Driver"),
    HSQL("jdbc:hsqldb:", "org.hsqldb.jdbcDriver"),
    INFORMIX("jdbc:informix-sqli:", "com.informix.jdbc.IfxDriver"),
    JTDS("jdbc:jtds:", "net.sourceforge.jtds.jdbc.Driver"),
    MARIADB("jdbc:mariadb:", "org.mariadb.jdbc.Driver"),
    MYSQL("jdbc:mysql:", "com.mysql.cj.jdbc.Driver"),
    MYSQL_GOOGLE("jdbc:google:", "com.mysql.jdbc.GoogleDriver"),
    ORACLE("jdbc:oracle", "oracle.jdbc.OracleDriver"),
    POSTGRESQL("jdbc:postgresql:", "org.postgresql.Driver"),
    REDSHIFT("jdbc:redshift:", "com.amazon.redshift.jdbc42.Driver"),
    SAPHANA("jdbc:sap:", "com.sap.db.jdbc.Driver"),
    SNOWFLAKE("jdbc:snowflake:", "net.snowflake.client.jdbc.SnowflakeDriver"),
    SQLDROID("jdbc:sqldroid:", "org.sqldroid.SQLDroidDriver"),
    SQLLITE("jdbc:sqlite:", "org.sqlite.JDBC"),
    SQLSERVER("jdbc:sqlserver:", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    SYBASE("jdbc:sybase:", "com.sybase.jdbc4.jdbc.SybDriver"),
    TEST_CONTAINERS("jdbc:tc:", "org.testcontainers.jdbc.ContainerDatabaseDriver");

    JdbcDriverType(String prefix, String driverClass) {
        this.prefix = prefix;
        this.driverClass = driverClass;
    }

    private final String prefix;
    private final String driverClass;

    public boolean matches(String url) {
        return url.startsWith(prefix);
    }
}
