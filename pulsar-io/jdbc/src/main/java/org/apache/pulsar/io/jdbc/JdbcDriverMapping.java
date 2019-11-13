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

import java.util.HashMap;
import java.util.Map;

public class JdbcDriverMapping {
    private static Map<String,String> DRIVER_MAPPING = new HashMap<>(32);
    static{
        DRIVER_MAPPING.put("postgresql","org.postgresql.Driver");
        DRIVER_MAPPING.put("sqlserver","com.microsoft.jdbc.sqlserver.SQLServerDriver");
        DRIVER_MAPPING.put("mysql","com.mysql.jdbc.Driver");
        DRIVER_MAPPING.put("mariadb","org.mariadb.jdbc.Driver");
        DRIVER_MAPPING.put("sqlite","org.sqlite.JDBC");
        DRIVER_MAPPING.put("clickhouse","ru.yandex.clickhouse.ClickHouseDriver");
    }
    public static String getDriverClass(String type){
        return DRIVER_MAPPING.get(type);
    }
}
