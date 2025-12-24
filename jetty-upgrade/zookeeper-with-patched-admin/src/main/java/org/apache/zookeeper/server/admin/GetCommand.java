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

import java.io.InputStream;
import java.util.List;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Command that represents HTTP GET request.
 */

public abstract class GetCommand extends CommandBase {

    protected GetCommand(List<String> names) {
        super(names);
    }

    protected GetCommand(List<String> names, boolean serverRequired) {
        super(names, serverRequired);
    }

    protected GetCommand(List<String> names, boolean serverRequired, AuthRequest authRequest) {
        super(names, serverRequired, authRequest);
    }

    @Override
    public CommandResponse runPost(ZooKeeperServer zkServer, InputStream inputStream) {
        return null;
    }
}
