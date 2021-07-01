---
id: version-2.6.4-io-canal-source
title: Canal source connector
sidebar_label: Canal source connector
original_id: io-canal-source
---

The Canal source connector pulls messages from MySQL to Pulsar topics.

## Configuration

The configuration of Canal source connector has the following properties.

### Property

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `username` | true | None | Canal server account (not MySQL).|
| `password` | true | None | Canal server password (not MySQL). |
|`destination`|true|None|Source destination that Canal source connector connects to.
| `singleHostname` | false | None | Canal server address.|
| `singlePort` | false | None | Canal server port.|
| `cluster` | true | false | Whether to enable cluster mode based on Canal server configuration or not.<br/><br/><li>true: **cluster** mode.<br/>If set to true, it talks to `zkServers` to figure out the actual database host.<br/><br/><li>false: **standalone** mode.<br/>If set to false, it connects to the database specified by `singleHostname` and `singlePort`. |
| `zkServers` | true | None | Address and port of the Zookeeper that Canal source connector talks to figure out the actual database host.|
| `batchSize` | false | 1000 | Batch size to fetch from Canal. |

### Example

Before using the Canal connector, you can create a configuration file through one of the following methods.

* JSON 

    ```json
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

* YAML

    You can create a YAML file and copy the [contents](https://github.com/apache/pulsar/blob/master/pulsar-io/canal/src/main/resources/canal-mysql-source-config.yaml) below to your YAML file.

    ```yaml
    configs:
        zkServers: "127.0.0.1:2181"
        batchSize: 5120
        destination: "example"
        username: ""
        password: ""
        cluster: false
        singleHostname: "127.0.0.1"
        singlePort: 11111
    ```

## Usage

Here is an example of storing MySQL data using the configuration file as above.

1. Start a MySQL server.

    ```bash
    $ docker pull mysql:5.7
    $ docker run -d -it --rm --name pulsar-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=canal -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw mysql:5.7
    ```

2. Create a configuration file `mysqld.cnf`.

    ```bash
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

3. Copy the configuration file `mysqld.cnf` to MySQL server.
   
    ```bash
    $ docker cp mysqld.cnf pulsar-mysql:/etc/mysql/mysql.conf.d/
    ```

4.  Restart the MySQL server.
   
    ```bash
    $ docker restart pulsar-mysql
    ```

5.  Create a test database in MySQL server.
   
    ```bash
    $ docker exec -it pulsar-mysql /bin/bash
    $ mysql -h 127.0.0.1 -uroot -pcanal -e 'create database test;'
    ```

6. Start a Canal server and connect to MySQL server.

    ```
    $ docker pull canal/canal-server:v1.1.2
    $ docker run -d -it --link pulsar-mysql -e canal.auto.scan=false -e canal.destinations=test -e canal.instance.master.address=pulsar-mysql:3306 -e canal.instance.dbUsername=root -e canal.instance.dbPassword=canal -e canal.instance.connectionCharset=UTF-8 -e canal.instance.tsdb.enable=true -e canal.instance.gtidon=false --name=pulsar-canal-server -p 8000:8000 -p 2222:2222 -p 11111:11111 -p 11112:11112 -m 4096m canal/canal-server:v1.1.2
    ```

7. Start Pulsar standalone.

    ```bash
    $ docker pull apachepulsar/pulsar:2.3.0
    $ docker run -d -it --link pulsar-canal-server -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --name pulsar-standalone apachepulsar/pulsar:2.3.0 bin/pulsar standalone
    ```

8. Modify the configuration file `canal-mysql-source-config.yaml`.

    ```yaml
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

9. Create a consumer file `pulsar-client.py`.

    ```python
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

10. Copy the configuration file `canal-mysql-source-config.yaml` and the consumer file  `pulsar-client.py` to Pulsar server.

    ```bash
    $ docker cp canal-mysql-source-config.yaml pulsar-standalone:/pulsar/conf/
    $ docker cp pulsar-client.py pulsar-standalone:/pulsar/
    ```

11. Download a Canal connector and start it.
    
    ```bash
    $ docker exec -it pulsar-standalone /bin/bash
    $ wget https://archive.apache.org/dist/pulsar/pulsar-2.3.0/connectors/pulsar-io-canal-2.3.0.nar -P connectors
    $ ./bin/pulsar-admin source localrun \
    --archive ./connectors/pulsar-io-canal-2.3.0.nar \
    --classname org.apache.pulsar.io.canal.CanalStringSource \
    --tenant public \
    --namespace default \
    --name canal \
    --destination-topic-name my-topic \
    --source-config-file /pulsar/conf/canal-mysql-source-config.yaml \
    --parallelism 1
    ```

12. Consume data from MySQL. 

    ```bash
    $ docker exec -it pulsar-standalone /bin/bash
    $ python pulsar-client.py
    ```

13. Open another window to log in MySQL server.

    ```bash
    $ docker exec -it pulsar-mysql /bin/bash
    $ mysql -h 127.0.0.1 -uroot -pcanal
    ```

14. Create a table, and insert, delete, and update data in MySQL server.
    
    ```bash
    mysql> use test;
    mysql> show tables;
    mysql> CREATE TABLE IF NOT EXISTS `test_table`(`test_id` INT UNSIGNED AUTO_INCREMENT,`test_title` VARCHAR(100) NOT NULL,
    `test_author` VARCHAR(40) NOT NULL,
    `test_date` DATE,PRIMARY KEY ( `test_id` ))ENGINE=InnoDB DEFAULT CHARSET=utf8;
    mysql> INSERT INTO test_table (test_title, test_author, test_date) VALUES("a", "b", NOW());
    mysql> UPDATE test_table SET test_title='c' WHERE test_title='a';
    mysql> DELETE FROM test_table WHERE test_title='c';
    ```

