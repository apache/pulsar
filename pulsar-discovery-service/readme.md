<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Discovery service

It keeps list of active available brokers and redirects all incoming requests to one of the broker in round-robin manner.

## Deployment

Discovery service module contains embedded jetty server and service can be started on it using following script:

```
mvn exec:java -Dexec.mainClass=org.apache.pulsar.discovery.service.server.DiscoveryServiceStarter -Dexec.args=$CONF_FILE
```

CONF_FILE should have following property
```
zookeeperServers=<zookeeper-connect-string(comma-separated)>
webServicePort=<port-to-start-server-on>
```



## TEST

After starting server: 

Hit any broker service url by providing discovery service domain.

Instead of calling individual broker url: 
```
http://broker-1:8080/admin/namespaces/{property}
```
 
call discovery service which redirects to one of the broker: 
```
http://pulsar-discovery-service:8080/admin/namespaces/{property}
```
Curl Example
```
curl -i -H "Accept: application/json" -H "Content-Type: application/json" -X GET http://pulsar-discovery:8080/admin/namespaces/{property} -L
```
