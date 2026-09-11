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
import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Interface implemented by all commands runnable by JettyAdminServer.
 *
 * @see CommandBase
 * @see Commands
 * @see JettyAdminServer
 */
public interface Command {

    /**
     * The set of all names that can be used to refer to this command (e.g.,
     * "configuration", "config", and "conf").
     */
    Set<String> getNames();

    /**
     * The name that is returned with the command response and that appears in
     * the list of all commands. This should be a member of the set returned by
     * getNames().
     */
    String getPrimaryName();

    /**
     * @return true if the command requires an active ZooKeeperServer or a
     * synced peer in order to resolve
     */
    boolean isServerRequired();

    /**
     * @return AuthRequest associated to the command. Null means auth check is not required.
     */
    AuthRequest getAuthRequest();

    /**
     * Run this command for HTTP GET request. Commands take a ZooKeeperServer, String-valued keyword
     * arguments and return a CommandResponse object containing any information
     * constituting the response to the command. Commands are responsible for
     * parsing keyword arguments and performing any error handling if necessary.
     * Errors should be reported by setting the "error" entry of the returned
     * map with an appropriate message rather than throwing an exception.
     *
     * @param zkServer ZooKeeper server
     * @param kwargs   keyword -&gt; argument value mapping
     * @return CommandResponse representing response to command containing at minimum:
     * - "command" key containing the command's primary name
     * - "error" key containing a String error message or null if no error
     */
    CommandResponse runGet(ZooKeeperServer zkServer, Map<String, String> kwargs);

    /**
     * Run this command for HTTP POST. Commands take a ZooKeeperServer and InputStream and
     * return a CommandResponse object containing any information
     * constituting the response to the command. Commands are responsible for
     * parsing keyword arguments and performing any error handling if necessary.
     * Errors should be reported by setting the "error" entry of the returned
     * map with an appropriate message rather than throwing an exception.
     *
     * @param zkServer    ZooKeeper server
     * @param inputStream InputStream from request
     * @return CommandResponse representing response to command containing at minimum:
     * - "command" key containing the command's primary name
     * - "error" key containing a String error message or null if no error
     */
    CommandResponse runPost(ZooKeeperServer zkServer, InputStream inputStream);
}
