---
id: sql-getting-started
title: 使用 Pulsar SQL 查询数据
sidebar_label: "查询数据"
---

在 Pulsar 中查询数据之前，您需要安装 Pulsar 和内置连接器。

## 必须
1. 安装 [Pulsar](getting-started-standalone.md#install-pulsar-standalone).
2. 安装 Pulsar 的 [内置连接器](getting-started-standalone.md#install-builtin-connectors-optional).

## 使用 Pulsar SQL 查询数据
您可以按照以下步骤使用 Pulsar SQL 查询数据

1. 首先，启动1个 Pulsar 单机集群

```bash

./bin/pulsar standalone

```

2. 然后，启动1个 Pulsar SQL worker 进程.

```bash

./bin/pulsar sql-worker run

```

3. 完成上述步骤后，创建1个 SQL 命令行工具.

```bash

./bin/pulsar sql

```

4. 此时，您可以使用以下指令进行 SQL 测试

```bash

presto> show catalogs;
 Catalog 
---------
 pulsar  
 system  
(2 rows)

Query 20180829_211752_00004_7qpwh, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]


presto> show schemas in pulsar;
        Schema         
-----------------------
 information_schema    
 public/default        
 public/functions
(3 rows)

Query 20180829_211818_00005_7qpwh, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:00 [4 rows, 89B] [21 rows/s, 471B/s]


presto> show tables in pulsar."public/default";
 Table 
-------
(0 rows)

Query 20180829_211839_00006_7qpwh, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]

```

因为在 Pulsar 中还没有任何数据，所以不会返回任何记录

5. 启动内置连接器 _DataGeneratorSource 并引入一些模拟数据。

```bash

./bin/pulsar-admin sources create --name generator --destinationTopicName generator_test --source-type data-generator

```

现在，您可以使用下面的指令来查询 "public/default" 命名空间下的主题了

```bash

presto> show tables in pulsar."public/default";
     Table      
----------------
 generator_test 
(1 row)

Query 20180829_213202_00000_csyeu, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:02 [1 rows, 38B] [0 rows/s, 17B/s]

```

当然，使用下面的指令，您也可以去查询 "generator_test" 主题中的数据

```bash

presto> select * from pulsar."public/default".generator_test;

  firstname  | middlename  |  lastname   |              email               |   username   | password | telephonenumber | age |                 companyemail                  | nationalidentitycardnumber | 
-------------+-------------+-------------+----------------------------------+--------------+----------+-----------------+-----+-----------------------------------------------+----------------------------+
 Genesis     | Katherine   | Wiley       | genesis.wiley@gmail.com          | genesisw     | y9D2dtU3 | 959-197-1860    |  71 | genesis.wiley@interdemconsulting.eu           | 880-58-9247                |   
 Brayden     |             | Stanton     | brayden.stanton@yahoo.com        | braydens     | ZnjmhXik | 220-027-867     |  81 | brayden.stanton@supermemo.eu                  | 604-60-7069                |   
 Benjamin    | Julian      | Velasquez   | benjamin.velasquez@yahoo.com     | benjaminv    | 8Bc7m3eb | 298-377-0062    |  21 | benjamin.velasquez@hostesltd.biz              | 213-32-5882                |   
 Michael     | Thomas      | Donovan     | donovan@mail.com                 | michaeld     | OqBm9MLs | 078-134-4685    |  55 | michael.donovan@memortech.eu                  | 443-30-3442                |   
 Brooklyn    | Avery       | Roach       | brooklynroach@yahoo.com          | broach       | IxtBLafO | 387-786-2998    |  68 | brooklyn.roach@warst.biz                      | 085-88-3973                |   
 Skylar      |             | Bradshaw    | skylarbradshaw@yahoo.com         | skylarb      | p6eC6cKy | 210-872-608     |  96 | skylar.bradshaw@flyhigh.eu                    | 453-46-0334                |    
.
.
.

```

此时您已经可以查询出模拟数据了

## 使用 Pulsar SQL 查询自建数据
在您查询自建数据之前，您应该先添加添加一些自己的数据。通过以下示例代码，您可以编写1个简单的消息生产者，进而发布自定义的消息到 Pulsar 中

```java

public class TestProducer {

    public static class Foo {
        private int field1 = 1;
        private String field2;
        private long field3;

        public Foo() {
        }

        public int getField1() {
            return field1;
        }

        public void setField1(int field1) {
            this.field1 = field1;
        }

        public String getField2() {
            return field2;
        }

        public void setField2(String field2) {
            this.field2 = field2;
        }

        public long getField3() {
            return field3;
        }

        public void setField3(long field3) {
            this.field3 = field3;
        }
    }

    public static void main(String[] args) throws Exception {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
        Producer<Foo> producer = pulsarClient.newProducer(AvroSchema.of(Foo.class)).topic("test_topic").create();

        for (int i = 0; i < 1000; i++) {
            Foo foo = new Foo();
            foo.setField1(i);
            foo.setField2("foo" + i);
            foo.setField3(System.currentTimeMillis());
            producer.newMessage().value(foo).send();
        }
        producer.close();
        pulsarClient.close();
    }
}

```

