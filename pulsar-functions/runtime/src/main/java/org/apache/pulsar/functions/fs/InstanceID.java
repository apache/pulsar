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

package org.apache.pulsar.functions.fs;

/**
 * Unique identifier for an execution of a function.
 */
public class InstanceID extends UUID {

    private static final long serialVersionUID = 1L;

    public static final InstanceID INVALID_INSTANCE_ID = new InstanceID(-1L, -1L);

    public InstanceID() {
        super();
    }

    public InstanceID(long lowestBits, long highestBits) {
        super(lowestBits, highestBits);
    }

    @Override
    public String toString() {
        return "instance-" + super.toString();
    }
}
