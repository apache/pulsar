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
package org.apache.pulsar.broker.service;

import lombok.Data;
import lombok.ToString;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ScanOutcome;

@Data
@ToString
public final class AnalyzeBacklogResult {

    private long entries;
    private long messages;

    private long filterRejectedEntries;
    private long filterAcceptedEntries;
    private long filterRescheduledEntries;

    private long filterRejectedMessages;
    private long filterAcceptedMessages;
    private long filterRescheduledMessages;

    private ScanOutcome scanOutcome;

    private Position firstPosition;
    private Position lastPosition;

}
