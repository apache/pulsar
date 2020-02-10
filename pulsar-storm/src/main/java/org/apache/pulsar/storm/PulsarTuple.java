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


import org.apache.storm.tuple.Values;

/**
 * Returned by MessageToValuesMapper, this specifies the Values
 * for an output tuple and the stream it should be sent to.
 */
public class PulsarTuple extends Values {

    protected final String outputStream;

    public PulsarTuple(String outStream, Object ... values) {
        super(values);
        outputStream = outStream;
    }

    /**
     * Return stream the tuple should be emitted on.
     *
     * @return String
     */
    public String getOutputStream() {
        return outputStream;
    }
}
