/*
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
package org.apache.zookeeper.server.admin;

import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * An AdminServer that does nothing.
 *
 * We use this class when we wish to disable the AdminServer. (This way we only
 * have to consider whether the server is enabled when we create the
 * AdminServer, which is handled by AdminServerFactory.)
 */
public class DummyAdminServer implements AdminServer {

    @Override
    public void start() throws AdminServerException {
    }

    @Override
    public void shutdown() throws AdminServerException {
    }

    @Override
    public void setZooKeeperServer(ZooKeeperServer zkServer) {
    }

}
