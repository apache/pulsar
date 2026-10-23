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
package org.apache.pulsar.client.impl;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/** Real partitioned child publication races the public parent resume operation. */
@JCStressTest
@Outcome(id = "2, 0", expect = Expect.ACCEPTABLE,
        desc = "All published children inherit the resumed parent state.")
@Outcome(expect = Expect.FORBIDDEN, desc = "A child is missing or retains an outdated pause state.")
@State
public class MultiTopicsPartitionSubscribeResumeStress {
    private final MultiTopicsPauseStressSupport fixture = new MultiTopicsPauseStressSupport(true, 2);

    @Actor
    public void subscribe() {
        fixture.subscribe();
    }

    @Actor
    public void resume() {
        fixture.parent.resume();
    }

    @Arbiter
    public void observe(II_Result result) {
        try {
            result.r1 = fixture.parent.consumers.size();
            result.r2 = fixture.mismatchedChildren(false);
        } finally {
            fixture.close();
        }
    }
}
