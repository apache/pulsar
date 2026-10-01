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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class CommandBase implements Command {

    private final String primaryName;
    private final Set<String> names;
    private final boolean serverRequired;
    private final AuthRequest authRequest;

    /**
     * @param names The possible names of this command, with the primary name first.
     */
    protected CommandBase(List<String> names) {
        this(names, true);
    }

    protected CommandBase(List<String> names, boolean serverRequired) {
        this(names, serverRequired, null);
    }

    protected CommandBase(List<String> names, boolean serverRequired, AuthRequest authRequest) {
        if (authRequest != null && !serverRequired) {
            throw new IllegalArgumentException("An active server is required for auth check");
        }
        this.primaryName = names.get(0);
        this.names = new HashSet<>(names);
        this.serverRequired = serverRequired;
        this.authRequest = authRequest;
    }

    @Override
    public String getPrimaryName() {
        return primaryName;
    }

    @Override
    public Set<String> getNames() {
        return names;
    }


    @Override
    public boolean isServerRequired() {
        return serverRequired;
    }

    @Override
    public AuthRequest getAuthRequest() {
        return authRequest;
    }

    /**
     * @return A response with the command set to the primary name and the
     * error set to null (these are the two entries that all command
     * responses are required to include).
     */
    protected CommandResponse initializeResponse() {
        return new CommandResponse(primaryName);
    }

}
