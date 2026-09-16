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
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;

/**
 * Gives the managed ledger access to what a {@link BookKeeper} client's configuration says about the client, whose
 * accessor is protected: this class lives in the client's package for that purpose.
 */
public final class BookKeeperClientConfigAccessor {

    private BookKeeperClientConfigAccessor() {
    }

    /**
     * Whether the client can issue batch reads: they require the v2 wire protocol and the client's own batch read
     * flag. A client without a configuration (a mock) cannot.
     */
    public static boolean supportsBatchRead(BookKeeper bookKeeper) {
        ClientConfiguration conf = bookKeeper.getConf();
        return conf != null && conf.getUseV2WireProtocol() && conf.isBatchReadEnabled();
    }
}
