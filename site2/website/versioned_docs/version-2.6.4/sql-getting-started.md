---
id: version-2.6.4-sql-getting-started
title: Query data with Pulsar SQL
sidebar_label: Query data
original_id: sql-getting-started
---

Before querying data in Pulsar, you need to install Pulsar and built-in connectors. 

## Requirements
1. Install [Pulsar](getting-started-standalone.md#install-pulsar-standalone).
2. Install Pulsar [built-in connectors](getting-started-standalone.md#install-builtin-connectors-optional).

## Query data in Pulsar
To query data in Pulsar with Pulsar SQL, complete the following steps.

1. Start a Pulsar standalone cluster.

```bash
./bin/pulsar standalone
```

2. Start a Pulsar SQL worker.

```bash
./bin/pulsar sql-worker run
```

3. After initializing Pulsar standalone cluster and the SQL worker, run SQL CLI.

```bash
./bin/pulsar sql
```

4. Test with SQL commands.

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
 sample/standalone/ns1 
(4 rows)

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

Since there is no data in Pulsar, no records is returned. 

5. Start the built-in connector _DataGeneratorSource_ and ingest some mock data.

```bash
./bin/pulsar-admin sources create --name generator --destinationTopicName generator_test --source-type data-generator
```

And then you can query a topic in the namespace "public/default".

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

You can now query the data within the topic "generator_test".

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

You can query the mock data.

## Query your own data
If you want to query your own data, you need to ingest your own data first. You can write a simple producer and write custom defined data to Pulsar. The following is an example. 

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
