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
package org.apache.pulsar.io.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Map;

/**
 * hbase connection Table
 */
public class TableUtils {

    public static Table getTable(Map<String, Object> config) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", config.get("zookeeperQuorum").toString());
        configuration.set("hbase.zookeeper.property.clientPort", config.get("zookeeperClientPort").toString());
        configuration.set("zookeeper.znode.parent", config.get("zookeeperZnodeParent").toString());

        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(config.get("tableName").toString());
        if (!admin.tableExists(tableName)) {
            throw new IllegalArgumentException(tableName + " table does not exist.");
        }
        return connection.getTable(tableName);
    }

}
