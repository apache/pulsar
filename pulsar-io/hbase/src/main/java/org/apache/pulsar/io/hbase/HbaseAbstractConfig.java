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

import com.google.common.base.Preconditions;
import java.io.Serializable;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration object for all Hbase Sink components.
 */
@Data
@Accessors(chain = true)
public class HbaseAbstractConfig implements Serializable {

    private static final long serialVersionUID = -8945930873383593712L;

    @FieldDoc(
        defaultValue = "hbase-site.xml",
        help = "hbase system configuration 'hbase-site.xml' file")
    private String hbaseConfigResources = "hbase-site.xml";

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "hbase system configuration about hbase.zookeeper.quorum value")
    private String zookeeperQuorum;

    @FieldDoc(
        defaultValue = "2181",
        help = "hbase system configuration about hbase.zookeeper.property.clientPort value")
    private String zookeeperClientPort = "2181";

    @FieldDoc(
        defaultValue = "/hbase",
        help = "hbase system configuration about zookeeper.znode.parent value")
    private String zookeeperZnodeParent = "/hbase";

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "hbase table, value is namespace:tableName, namespace default value is default")
    private String tableName;

    public void validate() {
        Preconditions.checkNotNull(zookeeperQuorum, "zookeeperQuorum property not set.");
        Preconditions.checkNotNull(zookeeperClientPort, "zookeeperClientPort property not set.");
        Preconditions.checkNotNull(zookeeperZnodeParent, "zookeeperZnodeParent property not set.");
        Preconditions.checkNotNull(tableName, "hbase tableName property not set.");
    }
}
