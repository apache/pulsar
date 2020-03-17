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
package org.apache.pulsar.proxy.stats;

import org.apache.pulsar.common.stats.Rate;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Getter;

@Getter
@JsonIgnoreProperties(value = { "msgInRate", "msgOutRate" })
public class TopicStats {

    double msgRateIn;
    double msgByteIn;
    double msgRateOut;
    double msgByteOut;

    Rate msgInRate;
    Rate msgOutRate;

    public TopicStats() {
        this.msgInRate = new Rate();
        this.msgOutRate = new Rate();
    }

    public void calculate() {
        msgInRate.calculateRate();
        msgOutRate.calculateRate();
        msgRateIn = msgInRate.getRate();
        msgByteIn = msgInRate.getValueRate();
        msgRateOut = msgOutRate.getRate();
        msgByteOut = msgOutRate.getValueRate();
    }

}
