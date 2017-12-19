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
package org.apache.pulsar.functions.runtime.container;

import lombok.*;
import org.apache.pulsar.functions.runtime.instance.JavaExecutionResult;
import org.apache.pulsar.functions.utils.Exceptions;

/**
 * An interface that represents the result of a function call.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
public class ExecutionResult {
    private String userException;
    private String systemException;
    private boolean timedOut;
    private byte[] result;
    public static ExecutionResult fromJavaResult(JavaExecutionResult result, SerDe serDe) {
        String userException = null;
        if (result.getUserException() != null ) {
            userException = Exceptions.toString(result.getUserException());
        }
        String systemException = null;
        if (result.getSystemException() != null ) {
            systemException = Exceptions.toString(result.getSystemException());
        }
        boolean timedOut = result.getTimeoutException() != null;
        byte[] output = null;
        if (result.getResult() != null) {
            output = serDe.serialize(result.getResult());
        }
        return new ExecutionResult(userException, systemException, timedOut, output);
    }
}