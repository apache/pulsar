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
package org.apache.pulsar.common.functions;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Configuration of a windowing function.
 */
@Data
@Accessors(chain = true)
public class WindowConfig {

    public static final String WINDOW_CONFIG_KEY = "__WINDOWCONFIGS__";

    private Integer windowLengthCount;

    private Long windowLengthDurationMs;

    private Integer slidingIntervalCount;

    private Long slidingIntervalDurationMs;

    private String lateDataTopic;

    private Long maxLagMs;

    private Long watermarkEmitIntervalMs;

    private String timestampExtractorClassName;

    private String actualWindowFunctionClassName;

    private ProcessingGuarantees processingGuarantees;

    /**
     * This is a semantic option that windows can provide,
     * forcing the semantics of the {@link FunctionConfig.ProcessingGuarantees} to be equal to MANUAL,
     * and then letting the windows function handle the semantic timing by itself.
     */
    public enum ProcessingGuarantees {
        ATLEAST_ONCE,
        ATMOST_ONCE
    }
}
