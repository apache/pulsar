package org.apache.pulsar.broker.service;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class AtomicIntegerFlags {
    private volatile int flags;
    private static final AtomicIntegerFieldUpdater<AtomicIntegerFlags> FLAGS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(
                    AtomicIntegerFlags.class, "flags");

    /**
     * enable/disable the flag at the specified index.
     *
     * @param index   the index of the flag in range of 0 to 30
     * @param enabled used to enable/disable the flag
     * @return true if the value was changed
     */
    public boolean changeFlag(int index, boolean enabled) {
        int bitMask = 1 << index;
        return ((FLAGS_UPDATER.getAndUpdate(this, currentValue -> {
            if (enabled) {
                return currentValue | bitMask;
            } else {
                return currentValue ^ bitMask;
            }
        }) & bitMask) == bitMask) == !enabled;
    }

    /**
     * get the enabled status of the flag.
     *
     * @param index the index of the flag in the range of 0 to 30
     * @return true if the flag is set
     */
    public boolean getFlag(int index) {
        int bitMask = 1 << index;
        return (flags & bitMask) == bitMask;
    }
}
