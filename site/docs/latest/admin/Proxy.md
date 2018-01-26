---
title: The Pulsar proxy
---

[`proxy`](../../reference/CliTools#pulsar-proxy) command

To start the proxy:

```bash
$ bin/pulsar proxy \
  --zookeeper-servers zk-0,zk-1,zk-2 \
  --global-zookeeper-servers zk-0,zk-1,zk-2
```

## Notes

* You can run multiple proxy nodes
* Stateless proxy over multiple brokers (all brokers in a cluster)
* Helpful when direct connectivity between clients and brokers is either not possible or not desirable (good way to avoid administrative overhead)
* Example: Kubernetes (clients can't access individual brokers and thus require a proxy to connect)
* All lookup and data connections flow through a proxy instance; those instances can be exposed via, for example, a load balancer
* Minimal interaction between client and broker