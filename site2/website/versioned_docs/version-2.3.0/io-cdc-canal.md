---
id: version-2.3.0-io-cdc-canal
title: CDC Canal Connector
sidebar_label: CDC Canal Connector
original_id: io-cdc-canal
---

### Source Configuration Options

The Configuration is mostly related to Canal task config.

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `zkServers` | `false` | `127.0.0.1:2181` | `The address and port of the zookeeper . if canal server configured to cluster mode` |
| `batchSize` | `true` | `5120` | `Take 5120 records from the canal server in batches` |
| `username` | `false` | `` | `Canal server account, not MySQL` |
| `password` | `false` | `` | `Canal server password, not MySQL` |
| `cluster` | `false` | `false` | `Decide whether to open cluster mode based on canal server configuration, true: cluster mode, false: standalone mode` |
| `singleHostname` | `false` | `127.0.0.1` | `The address of canal server` |
| `singlePort` | `false` | `11111` | `The port of canal server` |


### Configuration Example

Here is a configuration Json example:

```$json
{
    "zkServers": "127.0.0.1:2181",
    "batchSize": "5120",
    "destination": "example",
    "username": "",
    "password": "",
    "cluster": false,
    "singleHostname": "127.0.0.1",
    "singlePort": "11111",
}
```
You could also find the yaml example in this [file](https://github.com/apache/pulsar/blob/master/pulsar-io/canal/src/main/resources/canal-mysql-source-config.yaml), which has similar content below:

```$yaml
configs:
    zkServers: "127.0.0.1:2181"
    batchSize: "5120"
    destination: "example"
    username: ""
    password: ""
    cluster: false
    singleHostname: "127.0.0.1"
    singlePort: "11111"
```

### Usage example

Here is a simple example to store MySQL change data using above example config.

- Start a MySQL server

```$bash
docker pull mysql:5.7
docker run -d -it --rm --name pulsar-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=canal -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw mysql:5.7
```
- Modify configuration files mysqld.cnf

```
[mysqld]
pid-file    = /var/run/mysqld/mysqld.pid
socket      = /var/run/mysqld/mysqld.sock
datadir     = /var/lib/mysql
#log-error  = /var/log/mysql/error.log
# By default we only accept connections from localhost
#bind-address   = 127.0.0.1
# Disabling symbolic-links is recommended to prevent assorted security risks
symbolic-links=0
log-bin=mysql-bin
binlog-format=ROW
server_id=1
```

- Copy file to mysql server from local and restart mysql server
```$bash
docker cp mysqld.cnf pulsar-mysql:/etc/mysql/mysql.conf.d/
docker restart pulsar-mysql
```

- Create test database in mysql server
```$bash
docker exec -it pulsar-mysql /bin/bash
mysql -h 127.0.0.1 -uroot -pcanal -e 'create database test;'
```

- Start canal server and connect mysql server

```
docker pull canal/canal-server:v1.1.2
docker run -d -it --link pulsar-mysql -e canal.auto.scan=false -e canal.destinations=test -e canal.instance.master.address=pulsar-mysql:3306 -e canal.instance.dbUsername=root -e canal.instance.dbPassword=canal -e canal.instance.connectionCharset=UTF-8 -e canal.instance.tsdb.enable=true -e canal.instance.gtidon=false --name=pulsar-canal-server -p 8000:8000 -p 2222:2222 -p 11111:11111 -p 11112:11112 -m 4096m canal/canal-server:v1.1.2
```

- Start pulsar standalone

```$bash
docker pull apachepulsar/pulsar:2.3.0
docker run -d -it --link pulsar-canal-server -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --name pulsar-standalone apachepulsar/pulsar:2.3.0 bin/pulsar standalone
```

- Start pulsar-io in standalone

- Config file canal-mysql-source-config.yaml

```$yaml
configs:
    zkServers: ""
    batchSize: "5120"
    destination: "test"
    username: ""
    password: ""
    cluster: false
    singleHostname: "pulsar-canal-server"
    singlePort: "11111"
```
- Consumer file pulsar-client.py for test
```
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('my-topic',
                            subscription_name='my-sub')

while True:
    msg = consumer.receive()
    print("Received message: '%s'" % msg.data())
    consumer.acknowledge(msg)

client.close()
```

- Copy config file and test file to pulsar server

```$bash
docker cp canal-mysql-source-config.yaml pulsar-standalone:/pulsar/conf/
docker cp pulsar-client.py pulsar-standalone:/pulsar/
```

- Download canal connector and start canal connector
```$bash
docker exec -it pulsar-standalone /bin/bash
wget http://apache.01link.hk/pulsar/pulsar-2.3.0/connectors/pulsar-io-canal-2.3.0.nar -P connectors
./bin/pulsar-admin source localrun --archive ./connectors/pulsar-io-canal-2.3.0.nar --classname org.apache.pulsar.io.canal.CanalStringSource --tenant public --namespace default --name canal --destination-topic-name my-topic --source-config-file /pulsar/conf/canal-mysql-source-config.yaml --parallelism 1
```

- Consumption data 

```$bash
docker exec -it pulsar-standalone /bin/bash
python pulsar-client.py
```

- Open another window for login mysql server

```$bash
docker exec -it pulsar-mysql /bin/bash
mysql -h 127.0.0.1 -uroot -pcanal
```
- Create table and insert, delete, update data in mysql server
```
mysql> use test;
mysql> show tables;
mysql> CREATE TABLE IF NOT EXISTS `test_table`(`test_id` INT UNSIGNED AUTO_INCREMENT,`test_title` VARCHAR(100) NOT NULL,
`test_author` VARCHAR(40) NOT NULL,
`test_date` DATE,PRIMARY KEY ( `test_id` ))ENGINE=InnoDB DEFAULT CHARSET=utf8;
mysql> INSERT INTO test_table (test_title, test_author, test_date) VALUES("a", "b", NOW());
mysql> UPDATE test_table SET test_title='c' WHERE test_title='a';
mysql> DELETE FROM test_table WHERE test_title='c';
```

