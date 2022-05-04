package org.apache.pulsar.common.util;

public enum BitRateUnit {

    Bit {
        public double toBit(double bitRate) { return bitRate; }
        public double toKilobit(double bitRate) { return bitRate / C0; }
        public double toMegabit(double bitRate) { return bitRate / Math.pow(C0, 2); }
        public double toGigabit(double bitRate) { return bitRate / Math.pow(C0, 3); }
        public double toByte(double bitRate) { return bitRate / C1; }
        public double convert(double bitRate, BitRateUnit bitRateUnit) { return bitRateUnit.toBit(bitRate); }
    },
    Kilobit {
        public double toBit(double bitRate) { return bitRate * C0; }
        public double toKilobit(double bitRate) { return bitRate; }
        public double toMegabit(double bitRate) { return bitRate / C0; }
        public double toGigabit(double bitRate) { return bitRate / Math.pow(C0, 2); }
        public double toByte(double bitRate) { return bitRate * C0 / C1; }
        public double convert(double bitRate, BitRateUnit bitRateUnit) { return bitRateUnit.toKilobit(bitRate); }
    },
    Megabit {
        public double toBit(double bitRate) { return bitRate * Math.pow(C0, 2); }
        public double toKilobit(double bitRate) { return bitRate * C0; }
        public double toMegabit(double bitRate) { return bitRate; }
        public double toGigabit(double bitRate) { return bitRate / C0; }
        public double toByte(double bitRate) { return bitRate * Math.pow(C0, 2) / C1; }
        public double convert(double bitRate, BitRateUnit bitRateUnit) { return bitRateUnit.toMegabit(bitRate); }
    },
    Gigabit {
        public double toBit(double bitRate) { return bitRate * Math.pow(C0, 3); }
        public double toKilobit(double bitRate) { return bitRate * Math.pow(C0, 2); }
        public double toMegabit(double bitRate) { return bitRate * C0; }
        public double toGigabit(double bitRate) { return bitRate; }
        public double toByte(double bitRate) { return bitRate * Math.pow(C0, 3) / C1; }
        public double convert(double bitRate, BitRateUnit bitRateUnit) { return bitRateUnit.toGigabit(bitRate); }
    },
    Byte {
        public double toBit(double bitRate) { return bitRate * C1; }
        public double toKilobit(double bitRate) { return bitRate * C1 / C0; }
        public double toMegabit(double bitRate) { return bitRate * C1 / Math.pow(C0, 2); }
        public double toGigabit(double bitRate) { return bitRate * C1 / Math.pow(C0, 3); }
        public double toByte(double bitRate) { return bitRate; }
        public double convert(double bitRate, BitRateUnit bitRateUnit) { return bitRateUnit.toByte(bitRate); }
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
