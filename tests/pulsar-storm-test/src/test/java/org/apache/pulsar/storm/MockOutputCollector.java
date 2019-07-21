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
package org.apache.pulsar.storm;

import java.util.Collection;
import java.util.List;

import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.Tuple;

public class MockOutputCollector implements IOutputCollector {

    private boolean acked = false;
    private boolean failed = false;
    private Throwable lastError = null;
    private Tuple ackedTuple = null;
    private int numTuplesAcked = 0;

    @Override
    public void reportError(Throwable error) {
        lastError = error;
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
    }

    @Override
    public void ack(Tuple input) {
        acked = true;
        failed = false;
        ackedTuple = input;
        ++numTuplesAcked;
    }

    @Override
    public void fail(Tuple input) {
        failed = true;
        acked = false;
    }

    @Override
    public void resetTimeout(Tuple tuple) {

    }

    public boolean acked() {
        return acked;
    }

    public boolean failed() {
        return failed;
    }

    public Throwable getLastError() {
        return lastError;
    }

    public Tuple getAckedTuple() {
        return ackedTuple;
    }

    public int getNumTuplesAcked() {
        return numTuplesAcked;
    }

    public void reset() {
        acked = false;
        failed = false;
        lastError = null;
        ackedTuple = null;
        numTuplesAcked = 0;
    }

    @Override
    public void flush() {
        // Nothing to flush from buffer
    }

}
