package org.apache.pulsar.common.util;

public enum BitRateUnit {

    BPS {
        public double toBps(double bitRate) { return bitRate; }
        public double toKbps(double bitRate) { return bitRate / C0; }
        public double toMbps(double bitRate) { return bitRate / Math.pow(C0, 2); }
        public double toGbps(double bitRate) { return bitRate / Math.pow(C0, 3); }
        public double toByte(double bitRate) { return bitRate / C1; }
        public double convert(double bitRate, BitRateUnit bitRateUnit) { return bitRateUnit.toBps(bitRate); }
    },
    KBPS {
        public double toBps(double bitRate) { return bitRate * C0; }
        public double toKbps(double bitRate) { return bitRate; }
        public double toMbps(double bitRate) { return bitRate / C0; }
        public double toGbps(double bitRate) { return bitRate / Math.pow(C0, 2); }
        public double toByte(double bitRate) { return bitRate * C0 / C1; }
        public double convert(double bitRate, BitRateUnit bitRateUnit) { return bitRateUnit.toKbps(bitRate); }
    },
    MBPS {
        public double toBps(double bitRate) { return bitRate * Math.pow(C0, 2); }
        public double toKbps(double bitRate) { return bitRate * C0; }
        public double toMbps(double bitRate) { return bitRate; }
        public double toGbps(double bitRate) { return bitRate / C0; }
        public double toByte(double bitRate) { return bitRate * Math.pow(C0, 2) / C1; }
        public double convert(double bitRate, BitRateUnit bitRateUnit) { return bitRateUnit.toMbps(bitRate); }
    },
    GBPS {
        public double toBps(double bitRate) { return bitRate * Math.pow(C0, 3); }
        public double toKbps(double bitRate) { return bitRate * Math.pow(C0, 2); }
        public double toMbps(double bitRate) { return bitRate * C0; }
        public double toGbps(double bitRate) { return bitRate; }
        public double toByte(double bitRate) { return bitRate * Math.pow(C0, 3) / C1; }
        public double convert(double bitRate, BitRateUnit bitRateUnit) { return bitRateUnit.toGbps(bitRate); }
    },
    BYTE {
        public double toBps(double bitRate) { return bitRate * C1; }
        public double toKbps(double bitRate) { return bitRate * C1 / C0; }
        public double toMbps(double bitRate) { return bitRate * C1 / Math.pow(C0, 2); }
        public double toGbps(double bitRate) { return bitRate * C1 / Math.pow(C0, 3); }
        public double toByte(double bitRate) { return bitRate; }
        public double convert(double bitRate, BitRateUnit bitRateUnit) { return bitRateUnit.toByte(bitRate); }
    };

    static final int C0 = 1000;
    static final int C1 = 8;

    public double toBps(double bitRate) {
        throw new AbstractMethodError();
    }

    public double toKbps(double bitRate) {
        throw new AbstractMethodError();
    }

    public double toMbps(double bitRate) {
        throw new AbstractMethodError();
    }

    public double toGbps(double bitRate) {
        throw new AbstractMethodError();
    }

    public double toByte(double bitRate) {
        throw new AbstractMethodError();
    }
    public double convert(double bitRate, BitRateUnit bitRateUnit) {
        throw new AbstractMethodError();
    }
}
