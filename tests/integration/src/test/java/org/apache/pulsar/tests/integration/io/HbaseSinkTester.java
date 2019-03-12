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
package org.apache.pulsar.tests.integration.io;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.tests.integration.containers.HbaseContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.fail;

/**
 * A tester for testing hbase sink.
 */
@Slf4j
public class HbaseSinkTester extends SinkTester<HbaseContainer> {

   /**
    * A Simple class to test hbase class.
    */
   @Data
   @ToString
   @EqualsAndHashCode
   public static class Foo {
      private String rowKey;
      private String name;
      private String address;
      private int age;
      private boolean flag;
   }

   private String rowKeyName = "rowKey";
   private String familyName = "info";
   private String name = "name";
   private String address = "address";
   private String age = "age";
   private String flag = "flag";
   private AvroSchema<Foo> schema = AvroSchema.of(Foo.class);

   private Admin admin;
   private Table table;


   public HbaseSinkTester() {
      super(HbaseContainer.NAME, SinkType.HBASE);

      sinkConfig.put("zookeeperQuorum", HbaseContainer.NAME);
      sinkConfig.put("zookeeperClientPort", "2181");
      sinkConfig.put("tableName", "default:pulsar_hbase");
      sinkConfig.put("rowKeyName", rowKeyName);
      sinkConfig.put("familyName", familyName);

      List<String> qualifierNames = new ArrayList<>();
      qualifierNames.add(name);
      qualifierNames.add(address);
      qualifierNames.add(age);
      qualifierNames.add(flag);
      sinkConfig.put("qualifierNames",qualifierNames);
   }

   @Override
   public Schema<?> getInputTopicSchema() {
      return schema;
   }

   @Override
   protected HbaseContainer createSinkService(PulsarCluster cluster) {
      return new HbaseContainer(cluster.getClusterName());
   }

   @Override
   public void prepareSink() throws Exception {
      Configuration configuration = HBaseConfiguration.create();
      configuration.set("hbase.zookeeper.quorum", sinkConfig.get("zookeeperQuorum").toString());
      configuration.set("hbase.zookeeper.property.clientPort", sinkConfig.get("zookeeperClientPort").toString());
      Connection connection = ConnectionFactory.createConnection(configuration);
      log.info("connection: {}", connection);
      admin = connection.getAdmin();
      log.info("admin: {}", admin);
      TableName tableName = TableName.valueOf(sinkConfig.get("tableName").toString());
      if (!admin.tableExists(tableName)) {
         HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
         hTableDescriptor.addFamily(new HColumnDescriptor(familyName));
         admin.createTable(hTableDescriptor);
         log.info("create hbase table: {}" + tableName);
      }
      table = connection.getTable(tableName);
      log.info("hbase table: {}", table);
   }

   @Override
   public void validateSinkResult(Map<String, String> kvs) {
      String value = kvs.get("row_key");
      Foo obj = schema.decode(value.getBytes());
      log.info("kvs output obj: {}", obj);
      try {
         Get scan = new Get(Bytes.toBytes(value));
         Result result  = table.get(scan);
         log.info("table scan result: {}", result);
         byte[] byteName = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(name));
         byte[] byteAddress = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(address));
         byte[] byteAge = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(age));
         byte[] byteFlag = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(flag));
         Assert.assertEquals(obj.getName(), Bytes.toString(byteName));
         Assert.assertEquals(obj.getAddress(), Bytes.toString(byteAddress));
         Assert.assertEquals(obj.getAge(), Bytes.toInt(byteAge));
         Assert.assertEquals(obj.isFlag(), Bytes.toBoolean(byteFlag));

      } catch (IOException e) {
         log.error("hbase table scan rowKey " + value + "  exception: ", e);
         fail("hbase table scan rowKey failed.");
         return;
      }

   }
}
