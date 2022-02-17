---
id: version-2.3.1-sql-getting-started
title: Pulsar SQL Getting Started
sidebar_label: Getting Started
original_id: sql-getting-started
---

It is super easy to get started on querying data in Pulsar.  

## Requirements
1. **Pulsar distribution**
    * If you haven't install Pulsar, please reference [Installing Pulsar](io-quickstart.md#installing-pulsar)
2. **Pulsar built-in connectors**
    * If you haven't installed the built-in connectors, please reference [Installing Builtin Connectors](io-quickstart.md#installing-builtin-connectors)

First, start a Pulsar standalone cluster:

```bash
./bin/pulsar standalone
```

Next, start a Pulsar SQL worker:
```bash
./bin/pulsar sql-worker run
```

After both the Pulsar standalone cluster and the SQL worker are done initializing, run the SQL CLI:
```bash
./bin/pulsar sql
```

You can now start typing some SQL commands:


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

Currently, there is no data in Pulsar that we can query.  Lets start the built-in connector _DataGeneratorSource_ to ingest some mock data for us to query:

```bash
./bin/pulsar-admin source create --name generator --destinationTopicName generator_test --source-type data-generator
```

Afterwards, the will be a topic with can query in the namespace "public/default":

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

We can now query the data within the topic "generator_test":

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

Now, you have some mock data to query and play around with!

If you want to try to ingest some of your own data to play around with, you can write a simple producer to write custom defined data to Pulsar.

For example:

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

Afterwards, you should be able query the data you just wrote.
