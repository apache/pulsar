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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Interface for an embedded admin server that runs Commands. There is only one
 * functional implementation, JettyAdminServer. DummyAdminServer, which does
 * nothing, is used when we do not wish to run a server.
 */
@InterfaceAudience.Public
public interface AdminServer {

    void start() throws AdminServerException;

    void shutdown() throws AdminServerException;

    void setZooKeeperServer(ZooKeeperServer zkServer);

    @InterfaceAudience.Public
    class AdminServerException extends Exception {

        private static final long serialVersionUID = 1L;

        public AdminServerException(String message, Throwable cause) {
            super(message, cause);
        }

        public AdminServerException(Throwable cause) {
            super(cause);
        }

    }

}
