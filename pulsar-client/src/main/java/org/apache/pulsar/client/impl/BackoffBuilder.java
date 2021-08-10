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
package org.apache.pulsar.client.impl;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

public class BackoffBuilder {
    private long initial;
    private TimeUnit unitInitial;
    private long max;
    private TimeUnit unitMax;
    private Clock clock;
    private long mandatoryStop;
    private TimeUnit unitMandatoryStop;
    
    public BackoffBuilder() {
        this.initial = 0;
        this.max = 0;
        this.mandatoryStop = 0;
        this.clock = Clock.systemDefaultZone();
    }
    
    public BackoffBuilder setInitialTime(long initial, TimeUnit unitInitial) {
    	this.unitInitial = unitInitial;
    	this.initial = initial;
    	return this;
    }
    
    public BackoffBuilder setMax(long max, TimeUnit unitMax) {
    	this.unitMax = unitMax;
    	this.max = max;
    	return this;
    }
     
    public BackoffBuilder setMandatoryStop(long mandatoryStop, TimeUnit unitMandatoryStop) {
    	this.mandatoryStop = mandatoryStop;
    	this.unitMandatoryStop = unitMandatoryStop;
    	return this;
    }

    
    public Backoff create() {
    	return new Backoff(initial, unitInitial, max, unitMax, mandatoryStop, unitMandatoryStop, clock);
    }
}
