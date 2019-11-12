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
    private static Map<String,String> DRIVER_MAPPING = new HashMap<>();
    static{
        DRIVER_MAPPING.put("db2","com.ibm.db2.jcc.DB2Driver");
        DRIVER_MAPPING.put("postgresql","org.postgresql.Driver");
        DRIVER_MAPPING.put("hsqldb","org.hsqldb.jdbcDriver");
        DRIVER_MAPPING.put("sybase","com.sybase.jdbc.SybDriver");
        DRIVER_MAPPING.put("sqlserver","com.microsoft.jdbc.sqlserver.SQLServerDriver");
        DRIVER_MAPPING.put("jtds","net.sourceforge.jtds.jdbc.Driver");
        DRIVER_MAPPING.put("oracle","oracle.jdbc.OracleDriver");
        DRIVER_MAPPING.put("AliOracle","com.alibaba.jdbc.AlibabaDriver");
        DRIVER_MAPPING.put("mysql","com.mysql.jdbc.Driver");
        DRIVER_MAPPING.put("mariadb","org.mariadb.jdbc.Driver");
        DRIVER_MAPPING.put("derby","org.apache.derby.jdbc.EmbeddedDriver");
        DRIVER_MAPPING.put("hive","org.apache.hive.jdbc.HiveDriver");
        DRIVER_MAPPING.put("h2","org.h2.Driver");
        DRIVER_MAPPING.put("dm","dm.jdbc.driver.DmDriver");
        DRIVER_MAPPING.put("kingbase","com.kingbase.Driver");
        DRIVER_MAPPING.put("gbase","com.gbase.jdbc.Driver");
        DRIVER_MAPPING.put("xugu","com.xugu.cloudjdbc.Driver");
        DRIVER_MAPPING.put("oceanbase","com.mysql.jdbc.Driver");
        DRIVER_MAPPING.put("odps","com.aliyun.odps.jdbc.OdpsDriver");
        DRIVER_MAPPING.put("teradata","com.teradata.jdbc.TeraDriver");
        DRIVER_MAPPING.put("log4jdbc","net.sf.log4jdbc.DriverSpy");
        DRIVER_MAPPING.put("phoenix","org.apache.phoenix.jdbc.PhoenixDriver");
        DRIVER_MAPPING.put("edb","com.edb.Driver");
        DRIVER_MAPPING.put("kylin","org.apache.kylin.jdbc.Driver");
        DRIVER_MAPPING.put("sqlite","org.sqlite.JDBC");
        DRIVER_MAPPING.put("presto","com.facebook.presto.jdbc.PrestoDriver");
        DRIVER_MAPPING.put("elastic_search","com.alibaba.xdriver.elastic.jdbc.ElasticDriver");
        DRIVER_MAPPING.put("clickhouse","ru.yandex.clickhouse.ClickHouseDriver");
        DRIVER_MAPPING.put("presto","com.facebook.presto.jdbc.PrestoDriver");
        DRIVER_MAPPING.put("presto","com.facebook.presto.jdbc.PrestoDriver");
        DRIVER_MAPPING.put("presto","com.facebook.presto.jdbc.PrestoDriver");
    }
    public static String getDriverClass(String type){
        return DRIVER_MAPPING.get(type);
    }
}
