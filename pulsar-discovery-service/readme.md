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
