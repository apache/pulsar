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
package org.apache.pulsar.tests.performance.launcher;

import static org.apache.pulsar.tests.performance.launcher.JfrFlamegraphViews.View.ALLOC;
import static org.apache.pulsar.tests.performance.launcher.JfrFlamegraphViews.View.CPU;
import static org.apache.pulsar.tests.performance.launcher.JfrFlamegraphViews.View.LOCK;
import static org.apache.pulsar.tests.performance.launcher.JfrFlamegraphViews.View.WALL;
import static org.testng.Assert.assertEquals;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JfrFlamegraphViewsTest {
    @DataProvider
    public Object[][] options() {
        return new Object[][] {
                {"event=cpu,interval=10ms,alloc=2m,jfrsync=profile", Set.of(CPU, ALLOC)},
                {"event=cpu,interval=10ms,alloc=2m,jfrsync=profile,file=/profiles/a.jfr", Set.of(CPU, ALLOC)},
                {"event=wall,lock=1ms", Set.of(WALL, LOCK)},
                {"event=itimer", Set.of(CPU)},
                {"event=cpu,wall=10ms", Set.of(CPU, WALL)},
                {"alloc=512k", Set.of(ALLOC)},
                {"interval=10ms,jfrsync=profile", Set.of(CPU)},
                {"event=cache-misses", Set.of()},
                {"", Set.of()},
                {null, Set.of()},
        };
    }

    @Test(dataProvider = "options")
    public void configuredViews(String options, Set<JfrFlamegraphViews.View> expected) {
        assertEquals(JfrFlamegraphViews.configuredViews(options), expected);
    }
}
