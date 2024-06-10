package org.apache.pulsar.common.util.collections;

import org.roaringbitmap.RoaringBitmap;

import java.util.concurrent.locks.StampedLock;

public class ConcurrentRoaringBitmap extends RoaringBitmap {

    private final StampedLock lock = new StampedLock();

    @Override
    public void add(int x) {
        long stamp = lock.writeLock();
        try {
            super.add(x);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean contains(int x) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            if (super.contains(x)) {
                return true;
            }
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    return super.contains(x);
                } finally {
                    lock.unlockRead(stamp);
                }
            }
        } else {
            stamp = lock.readLock();
            try {
                return super.contains(x);
            } finally {
                lock.unlockRead(stamp);
            }
        }
        return false;
    }

    @Override
    public void remove(int x) {
        long stamp = lock.writeLock();
        try {
            super.remove(x);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void clear() {
        long stamp = lock.writeLock();
        try {
            super.clear();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public int getSizeInBytes() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            int size = super.getSizeInBytes();
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    return super.getSizeInBytes();
                } finally {
                    lock.unlockRead(stamp);
                }
            }
            return size;
        } else {
            stamp = lock.readLock();
            try {
                return super.getSizeInBytes();
            } finally {
                lock.unlockRead(stamp);
            }
        }
    }

    @Override
    public ConcurrentRoaringBitmap clone() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            ConcurrentRoaringBitmap clone = (ConcurrentRoaringBitmap) super.clone();
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    return (ConcurrentRoaringBitmap) super.clone();
                } finally {
                    lock.unlockRead(stamp);
                }
            }
            return clone;
        } else {
            stamp = lock.readLock();
            try {
                return (ConcurrentRoaringBitmap) super.clone();
            } finally {
                lock.unlockRead(stamp);
            }
        }
    }

    @Override
    public void and(RoaringBitmap x2) {
        long stamp = lock.writeLock();
        try {
            super.and(x2);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void or(RoaringBitmap x2) {
        long stamp = lock.writeLock();
        try {
            super.or(x2);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void xor(RoaringBitmap x2) {
        long stamp = lock.writeLock();
        try {
            super.xor(x2);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void andNot(RoaringBitmap x2) {
        long stamp = lock.writeLock();
        try {
            super.andNot(x2);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void flip(int rangeStart, int rangeEnd) {
        long stamp = lock.writeLock();
        try {
            super.flip(rangeStart, rangeEnd);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void flip(int x) {
        long stamp = lock.writeLock();
        try {
            super.flip(x);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void serialize(java.io.DataOutput out) throws java.io.IOException {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            super.serialize(out);
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    super.serialize(out);
                } finally {
                    lock.unlockRead(stamp);
                }
            }
        } else {
            stamp = lock.readLock();
            try {
                super.serialize(out);
            } finally {
                lock.unlockRead(stamp);
            }
        }
    }

    @Override
    public void deserialize(java.io.DataInput in) throws java.io.IOException {
        long stamp = lock.writeLock();
        try {
            super.deserialize(in);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void readExternal(java.io.ObjectInput in) throws java.io.IOException {
        long stamp = lock.writeLock();
        try {
            super.readExternal(in);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void writeExternal(java.io.ObjectOutput out) throws java.io.IOException {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            super.writeExternal(out);
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    super.writeExternal(out);
                } finally {
                    lock.unlockRead(stamp);
                }
            }
        } else {
            stamp = lock.readLock();
            try {
                super.writeExternal(out);
            } finally {
                lock.unlockRead(stamp);
            }
        }
    }
}
