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
package org.apache.pulsar.broker;

public enum BitRateUnit {

    Bit {
        public double toBit(double bitRate) {
            return bitRate;
        }

        public double toKilobit(double bitRate) {
            return bitRate / C0;
        }

        public double toMegabit(double bitRate) {
            return bitRate / Math.pow(C0, 2);
        }

        public double toGigabit(double bitRate) {
            return bitRate / Math.pow(C0, 3);
        }

        public double toByte(double bitRate) {
            return bitRate / C1;
        }

        public double convert(double bitRate, BitRateUnit bitRateUnit) {
            return bitRateUnit.toBit(bitRate);
        }
    },
    Kilobit {
        public double toBit(double bitRate) {
            return bitRate * C0;
        }

        public double toKilobit(double bitRate) {
            return bitRate;
        }

        public double toMegabit(double bitRate) {
            return bitRate / C0;
        }

        public double toGigabit(double bitRate) {
            return bitRate / Math.pow(C0, 2);
        }

        public double toByte(double bitRate) {
            return bitRate * C0 / C1;
        }

        public double convert(double bitRate, BitRateUnit bitRateUnit) {
            return bitRateUnit.toKilobit(bitRate);
        }
    },
    Megabit {
        public double toBit(double bitRate) {
            return bitRate * Math.pow(C0, 2);
        }

        public double toKilobit(double bitRate) {
            return bitRate * C0;
        }

        public double toMegabit(double bitRate) {
            return bitRate;
        }

        public double toGigabit(double bitRate) {
            return bitRate / C0;
        }

        public double toByte(double bitRate) {
            return bitRate * Math.pow(C0, 2) / C1;
        }

        public double convert(double bitRate, BitRateUnit bitRateUnit) {
            return bitRateUnit.toMegabit(bitRate);
        }
    },
    Gigabit {
        public double toBit(double bitRate) {
            return bitRate * Math.pow(C0, 3);
        }

        public double toKilobit(double bitRate) {
            return bitRate * Math.pow(C0, 2);
        }

        public double toMegabit(double bitRate) {
            return bitRate * C0;
        }

        public double toGigabit(double bitRate) {
            return bitRate;
        }

        public double toByte(double bitRate) {
            return bitRate * Math.pow(C0, 3) / C1;
        }

        public double convert(double bitRate, BitRateUnit bitRateUnit) {
            return bitRateUnit.toGigabit(bitRate);
        }
    },
    Byte {
        public double toBit(double bitRate) {
            return bitRate * C1;
        }

        public double toKilobit(double bitRate) {
            return bitRate * C1 / C0;
        }

        public double toMegabit(double bitRate) {
            return bitRate * C1 / Math.pow(C0, 2);
        }

        public double toGigabit(double bitRate) {
            return bitRate * C1 / Math.pow(C0, 3);
        }

        public double toByte(double bitRate) {
            return bitRate;
        }

        public double convert(double bitRate, BitRateUnit bitRateUnit) {
            return bitRateUnit.toByte(bitRate);
        }
    };

    static final int C0 = 1000;
    static final int C1 = 8;

    public double toBit(double bitRate) {
        throw new AbstractMethodError();
    }

    public double toKilobit(double bitRate) {
        throw new AbstractMethodError();
    }

    public double toMegabit(double bitRate) {
        throw new AbstractMethodError();
    }

    public double toGigabit(double bitRate) {
        throw new AbstractMethodError();
    }

    public double toByte(double bitRate) {
        throw new AbstractMethodError();
    }

    public double convert(double bitRate, BitRateUnit bitRateUnit) {
        throw new AbstractMethodError();
    }
}
