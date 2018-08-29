---
id: sql-deployment-configuration
title: Pulsar SQl Deployment and Configuration
sidebar_label: Deployment and Configuration
---

Pulsar SQL is powered by [Presto](https://prestodb.io) thus many of the configurations for deployment is the same for the Pulsar SQL worker.

Thus, you can use the same CLI args as the Presto launcher:

```bash
./bin/pulsar sql-worker --help
Usage: launcher [options] command

Commands: run, start, stop, restart, kill, status

Options:
  -h, --help            show this help message and exit
  -v, --verbose         Run verbosely
  --etc-dir=DIR         Defaults to INSTALL_PATH/etc
  --launcher-config=FILE
                        Defaults to INSTALL_PATH/bin/launcher.properties
  --node-config=FILE    Defaults to ETC_DIR/node.properties
  --jvm-config=FILE     Defaults to ETC_DIR/jvm.config
  --config=FILE         Defaults to ETC_DIR/config.properties
  --log-levels-file=FILE
                        Defaults to ETC_DIR/log.properties
  --data-dir=DIR        Defaults to INSTALL_PATH
  --pid-file=FILE       Defaults to DATA_DIR/var/run/launcher.pid
  --launcher-log-file=FILE
                        Defaults to DATA_DIR/var/log/launcher.log (only in
                        daemon mode)
  --server-log-file=FILE
                        Defaults to DATA_DIR/var/log/server.log (only in
                        daemon mode)
  -D NAME=VALUE         Set a Java system property

```

There is a set of default configs for the cluster located in ```${project.root}/conf/presto``` that will be used by default

To start multiple Presto workers, you can set the worker to read from a different configuration directory as well as set a different directory for writing its data:

```bash
./bin/pulsar sql-worker --etc-dir /tmp/incubator-pulsar/conf/presto --data-dir /tmp/presto-1
```

For more information about deployment in Presto, please reference:

[Deploying Presto](https://prestodb.io/docs/current/installation/deployment.html)

