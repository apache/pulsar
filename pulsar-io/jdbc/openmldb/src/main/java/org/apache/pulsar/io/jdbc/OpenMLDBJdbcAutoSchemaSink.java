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

import java.sql.PreparedStatement;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
        name = "jdbc-openmldb",
        type = IOType.SINK,
        help = "A simple JDBC sink for OpenMLDB that writes pulsar messages to a database table",
        configClass = JdbcSinkConfig.class
)
public class OpenMLDBJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

    /**
     * OpenMLDB does not support native array types.
     * <p>
     * OpenMLDB is focused on time-series and real-time analytics workloads and does not
     * provide native array data types like PostgreSQL. This implementation does not
     * provide automatic array conversion to maintain consistency with OpenMLDB's
     * data model and type system.
     * </p>
     * <p>
     * <strong>Alternatives:</strong>
     * <ul>
     * <li>Use PostgreSQL JDBC sink for native array support</li>
     * <li>Serialize arrays to JSON strings for storage in STRING columns</li>
     * <li>Use separate tables with foreign keys for one-to-many relationships</li>
     * <li>Flatten array data into multiple rows with appropriate indexing</li>
     * </ul>
     * </p>
     *
     * @param statement     the PreparedStatement (not used)
     * @param index         the parameter index (not used)
     * @param arrayValue    the array value (not used)
     * @param targetSqlType the target SQL type (not used)
     * @throws UnsupportedOperationException always thrown as OpenMLDB doesn't support arrays
     */
    @Override
    protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue, String targetSqlType)
            throws Exception {
        throw new UnsupportedOperationException("Array types are not supported by OpenMLDB JDBC sink. "
                + "Consider using PostgreSQL JDBC sink for array support.");
    }
}
