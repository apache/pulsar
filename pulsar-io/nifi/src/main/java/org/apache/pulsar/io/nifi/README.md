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

## Apache NiFi Connectors for Pulsar

Pulsar uses the NiFi Site-to-Site client pulls data from Apache NiFi and delivers data to Apache NiFi.

## Example

1. set conf/nifi.properties, like  [configure-site-to-site](http://nifi.apache.org/docs/nifi-docs/html/user-guide.html#configure-site-to-site-server-nifi-instance)

```file
nifi.remote.input.socket.port=10443
```
2. start nifi

```shell
bin/nifi.sh start
```
3. open a web browser and navigate to http://localhost:8080/nifi 

### NiFiSink

Sample NiFi receive data from Pulsar:

```file
1 chose Input Port and set Port Name, The Port Name corresponding to pulsar NiFiSink portName.
2 add Processor PutFile and config.
3 start NiFi Flow
```

### NiFiSource

Sample NiFi receive data from Pulsar:

```file
1 add Processor GutFile and config.
2 chose Output Port and set Port Name, The Port Name corresponding to pulsar NiFiSource portName.
3 start NiFi Flow
```
