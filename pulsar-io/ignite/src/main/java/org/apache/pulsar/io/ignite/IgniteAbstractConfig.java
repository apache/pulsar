/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.ignite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.Serializable;
import java.util.List;

/**
 * Configuration object for all ignite Sink components.
 */
@Data
@Accessors(chain = true)
public class IgniteAbstractConfig implements Serializable {

    private static final long serialVersionUID = -7860917032537872317L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "A comma separated list of ignite hosts to connect to")
    private String igniteHosts;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "The password used to connect to ignite")
    private String ignitePassword;

    @FieldDoc(
        required = true,
        defaultValue = "0",
        help = "The ignite database to connect to")
    private int igniteDatabase = 0;

    @FieldDoc(
        required = false,
        defaultValue = "Standalone",
        help = "The client mode to use when interacting with the ignite cluster. Possible values [Standalone, Cluster]")
    private String clientMode = "Standalone";

    @FieldDoc(
        required = false,
        defaultValue = "true",
        help = "Flag to determine if the ignite client should automatically reconnect")
    private boolean autoReconnect = true;

    @FieldDoc(
        required = false,
        defaultValue = "2147483647",
        help = "The maximum number of queued requests to ignite")
    private int requestQueue = 2147483647;

    @FieldDoc(
        required = false,
        defaultValue = "false",
        help = "Flag to enable TCP no delay should be used")
    private boolean tcpNoDelay = false;

    @FieldDoc(
        required = false,
        defaultValue = "false",
        help = "Flag to enable a keepalive to ignite")
    private boolean keepAlive = false;

    @FieldDoc(
        required = false,
        defaultValue = "10000L",
        help = "The amount of time in milliseconds to wait before timing out when connecting")
    private long connectTimeout = 10000L;

    public void validate() {
        Preconditions.checkNotNull(igniteHosts, "igniteHosts property not set.");
        Preconditions.checkNotNull(igniteDatabase, "igniteDatabase property not set.");
        Preconditions.checkNotNull(clientMode, "clientMode property not set.");
    }

    public enum ClientMode {
        STANDALONE,
        CLUSTER
    }

    public List<HostAndPort> getHostAndPorts() {
        List<HostAndPort> hostAndPorts = Lists.newArrayList();;
        Preconditions.checkNotNull(igniteHosts, "igniteHosts property not set.");
        String[] hosts = StringUtils.split(igniteHosts, ",");
        for (String host : hosts) {
            HostAndPort hostAndPort = HostAndPort.fromString(host);
            hostAndPorts.add(hostAndPort);
        }
        return hostAndPorts;
    }
}
