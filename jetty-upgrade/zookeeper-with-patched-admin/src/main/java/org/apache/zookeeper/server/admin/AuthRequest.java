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

import org.apache.zookeeper.ZooDefs;

/**
 * Class representing auth data for performing ACL check on admin server commands.
 *
 * For example, SnapshotCommand requires {@link ZooDefs.Perms.ALL} permission on
 * the root node.
 *
 */
public class AuthRequest {
    private final int permission;
    private final String path;

    /**
     * @param permission the required permission for auth check
     * @param path       the ZNode path for auth check
     */
    public AuthRequest(final int permission, final String path) {
        this.permission = permission;
        this.path = path;
    }

    /**
     * @return permission
     */
    public int getPermission() {
        return permission;
    }

    /**
     * @return ZNode path
     */
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "AuthRequest{"
                + "permission=" + permission
                + ", path='" + path + '\''
                + '}';
    }
}
