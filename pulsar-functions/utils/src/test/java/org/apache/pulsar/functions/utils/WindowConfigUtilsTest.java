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

package org.apache.pulsar.functions.utils;

import org.apache.pulsar.common.functions.WindowConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

/**
 * Unit test of {@link Exceptions}.
 */
public class WindowConfigUtilsTest {

    @Test
    public void testSettingSlidingCountWindow() throws Exception {
        final Object[][] args = new Object[][]{
                {-1, 10},
                {10, -1},
                {0, 10},
                {10, 0},
                {0, 0},
                {-1, -1},
                {5, 10},
                {1, 1},
                {10, 5},
                {100, 10},
                {100, 100},
                {200, 100},
                {500, 100},
                {null, null},
                {null, 1},
                {1, null},
                {null, -1},
                {-1, null}
        };

        for (Object[] arg : args) {
            Object arg0 = arg[0];
            Object arg1 = arg[1];
            try {

                Integer windowLengthCount = null;
                if (arg0 != null) {
                    windowLengthCount = (Integer) arg0;
                }
                Integer slidingIntervalCount = null;
                if (arg1 != null) {
                    slidingIntervalCount = (Integer) arg1;
                }
                WindowConfig windowConfig = new WindowConfig();
                windowConfig.setWindowLengthCount(windowLengthCount);
                windowConfig.setSlidingIntervalCount(slidingIntervalCount);

                WindowConfigUtils.validate(windowConfig);

                if (arg0 == null) {
                    fail(String.format("Window length cannot be null -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }
                if ((Integer) arg0 <= 0) {
                    fail(String.format("Window length cannot be zero or less -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }
                if (arg1 != null && (Integer) arg1 <= 0) {
                    fail(String.format("Sliding interval length cannot be zero or less -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }
            } catch (IllegalArgumentException e) {
                if (arg0 != null && arg1 != null && (Integer) arg0 > 0 && (Integer) arg1 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- windowLengthCount: %s "
                            + "slidingIntervalCount: %s", e.getMessage(), arg0, arg1));
                }
            }
        }
    }

    @Test
    public void testSettingSlidingTimeWindow() throws Exception {
        final Object[][] args = new Object[][]{
                {-1L, 10L},
                {10L, -1L},
                {0L, 10L},
                {10L, 0L},
                {0L, 0L},
                {-1L, -1L},
                {5L, 10L},
                {1L, 1L},
                {10L, 5L},
                {100L, 10L},
                {100L, 100L},
                {200L, 100L},
                {500L, 100L},
                {null, null},
                {null, 1L},
                {1L, null},
                {null, -1L},
                {-1L, null}
        };

        for (Object[] arg : args) {
            Object arg0 = arg[0];
            Object arg1 = arg[1];
            try {
                Long windowLengthDuration = null;
                if (arg0 != null) {
                    windowLengthDuration = (Long) arg0;
                }
                Long slidingIntervalDuration = null;
                if (arg1 != null) {
                    slidingIntervalDuration = (Long) arg1;
                }

                WindowConfig windowConfig = new WindowConfig();
                windowConfig.setWindowLengthDurationMs(windowLengthDuration);
                windowConfig.setSlidingIntervalDurationMs(slidingIntervalDuration);
                WindowConfigUtils.validate(windowConfig);

                if (arg0 == null) {
                    fail(String.format("Window length cannot be null -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }
                if ((Long) arg0 <= 0) {
                    fail(String.format("Window length cannot be zero or less -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }
                if (arg1 != null && (Long) arg1 <= 0) {
                    fail(String.format("Sliding interval length cannot be zero or less -- "
                            + "windowLengthCount: %s slidingIntervalCount: %s", arg0, arg1));
                }
            } catch (IllegalArgumentException e) {
                if (arg0 != null && arg1 != null && (Long) arg0 > 0 && (Long) arg1 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- windowLengthDuration: %s "
                            + "slidingIntervalDuration: %s", e.getMessage(), arg0, arg1));
                }
            }
        }
    }


    @Test
    public void testSettingTumblingCountWindow() throws Exception {
        final Object[] args = new Object[]{-1, 0, 1, 2, 5, 10, null};

        for (Object arg : args) {
            Object arg0 = arg;
            try {

                Integer windowLengthCount = null;
                if (arg0 != null) {
                    windowLengthCount = (Integer) arg0;
                }

                WindowConfig windowConfig = new WindowConfig();
                windowConfig.setWindowLengthCount(windowLengthCount);
                WindowConfigUtils.validate(windowConfig);

                if (arg0 == null) {
                    fail(String.format("Window length cannot be null -- windowLengthCount: %s", arg0));
                }
                if ((Integer) arg0 <= 0) {
                    fail(String.format("Window length cannot be zero or less -- windowLengthCount: %s",
                            arg0));
                }
            } catch (IllegalArgumentException e) {
                if (arg0 != null && (Integer) arg0 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- windowLengthCount: %s", e
                            .getMessage(), arg0));
                }
            }
        }
    }

    @Test
    public void testSettingTumblingTimeWindow() throws Exception {
        final Object[] args = new Object[]{-1L, 0L, 1L, 2L, 5L, 10L, null};
        for (Object arg : args) {
            Object arg0 = arg;
            try {

                Long windowLengthDuration = null;
                if (arg0 != null) {
                    windowLengthDuration = (Long) arg0;
                }

                WindowConfig windowConfig = new WindowConfig();
                windowConfig.setWindowLengthDurationMs(windowLengthDuration);
                WindowConfigUtils.validate(windowConfig);

                if (arg0 == null) {
                    fail(String.format("Window count duration cannot be null -- windowLengthDuration: %s",
                            arg0));
                }
                if ((Long) arg0 <= 0) {
                    fail(String.format("Window length cannot be zero or less -- windowLengthDuration: %s",
                            arg0));
                }
            } catch (IllegalArgumentException e) {
                if (arg0 != null && (Long) arg0 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- windowLengthDuration: %s", e
                            .getMessage(), arg0));
                }
            }
        }
    }

    @Test
    public void testSettingLagTime() throws Exception {
        final Object[] args = new Object[]{-1L, 0L, 1L, 2L, 5L, 10L, null};
        for (Object arg : args) {
            Object arg0 = arg;
            try {

                Long maxLagMs = null;
                if (arg0 != null) {
                    maxLagMs = (Long) arg0;
                }

                WindowConfig windowConfig = new WindowConfig();
                windowConfig.setWindowLengthCount(1);
                windowConfig.setSlidingIntervalCount(1);
                windowConfig.setMaxLagMs(maxLagMs);
                windowConfig.setTimestampExtractorClassName("SomeClass");
                WindowConfigUtils.validate(windowConfig);

                if(arg0 != null && (Long) arg0 < 0) {
                    fail(String.format("Window lag cannot be less than zero -- lagTime: %s", arg0));
                }
            } catch (IllegalArgumentException e) {
                if (arg0 != null && (Long) arg0 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- lagTime: %s",
                            e.getMessage(), arg0));
                }
            }
        }
    }

    @Test
    public void testSettingWaterMarkInterval() throws Exception {
        final Object[] args = new Object[]{-1L, 0L, 1L, 2L, 5L, 10L, null};
        for (Object arg : args) {
            Object arg0 = arg;
            try {
                Long watermarkEmitInterval = null;
                if (arg0 != null) {
                    watermarkEmitInterval = (Long) arg0;
                }

                WindowConfig windowConfig = new WindowConfig();
                windowConfig.setWindowLengthCount(1);
                windowConfig.setSlidingIntervalCount(1);
                windowConfig.setWatermarkEmitIntervalMs(watermarkEmitInterval);
                windowConfig.setTimestampExtractorClassName("SomeClass");
                WindowConfigUtils.validate(windowConfig);

                if (arg0 != null && (Long) arg0 <= 0) {
                    fail(String.format("Watermark interval cannot be zero or less -- watermarkInterval: "
                            + "%s", arg0));
                }
            } catch (IllegalArgumentException e) {
                if (arg0 != null && (Long) arg0 > 0) {
                    fail(String.format("Exception: %s thrown on valid input -- watermarkInterval: %s", e
                            .getMessage(), arg0));
                }
            }
        }
    }
}
