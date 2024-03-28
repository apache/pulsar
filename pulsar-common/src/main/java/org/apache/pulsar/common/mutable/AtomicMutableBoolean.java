package org.apache.pulsar.common.mutable;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;

public class AtomicMutableBoolean extends MutableBoolean {

    private volatile boolean volatileValue;

    public AtomicMutableBoolean() {
        this(false);
    }

    public AtomicMutableBoolean(final boolean value) {
        super(value);
        this.volatileValue = value;
    }

    public AtomicMutableBoolean(final Boolean volatileValue) {
        super(volatileValue);
        this.volatileValue = volatileValue.booleanValue();
    }

    //-----------------------------------------------------------------------
    /**
     * Gets the value as a Boolean instance.
     *
     * @return the value as a Boolean, never null
     */
    @Override
    public Boolean getValue() {
        return Boolean.valueOf(this.volatileValue);
    }

    /**
     * Sets the value.
     *
     * @param value  the value to set
     */
    @Override
    public void setValue(final boolean value) {
        this.volatileValue = value;
    }

    /**
     * Sets the value to false.
     */
    @Override
    public void setFalse() {
        this.volatileValue = false;
    }

    /**
     * Sets the value to true.
     */
    @Override
    public void setTrue() {
        this.volatileValue = true;
    }

    /**
     * Sets the value from any Boolean instance.
     *
     * @param value  the value to set, not null.
     * @throws NullPointerException if the object is null.
     */
    @Override
    public void setValue(final Boolean value) {
        this.volatileValue = value.booleanValue();
    }

    //-----------------------------------------------------------------------
    /**
     * Checks if the current value is {@code true}.
     *
     * @return {@code true} if the current value is {@code true}
     */
    @Override
    public boolean isTrue() {
        return volatileValue;
    }

    /**
     * Checks if the current value is {@code false}.
     *
     * @return {@code true} if the current value is {@code false}
     */
    @Override
    public boolean isFalse() {
        return !volatileValue;
    }

    //-----------------------------------------------------------------------
    /**
     * Returns the value of this MutableBoolean as a boolean.
     *
     * @return the boolean value represented by this object.
     */
    @Override
    public boolean booleanValue() {
        return volatileValue;
    }

    //-----------------------------------------------------------------------
    /**
     * Gets this mutable as an instance of Boolean.
     *
     * @return a Boolean instance containing the value from this mutable, never null
     */
    @Override
    public Boolean toBoolean() {
        return Boolean.valueOf(booleanValue());
    }

    //-----------------------------------------------------------------------
    /**
     * Compares this object to the specified object. The result is {@code true} if and only if the argument is
     * not {@code null} and is an {@code MutableBoolean} object that contains the same
     * {@code boolean} value as this object.
     *
     * @param obj  the object to compare with, null returns false
     * @return {@code true} if the objects are the same; {@code false} otherwise.
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof MutableBoolean) {
            return volatileValue == ((MutableBoolean) obj).booleanValue();
        }
        return false;
    }

    /**
     * Returns a suitable hash code for this mutable.
     *
     * @return the hash code returned by {@code Boolean.TRUE} or {@code Boolean.FALSE}
     */
    @Override
    public int hashCode() {
        return volatileValue ? Boolean.TRUE.hashCode() : Boolean.FALSE.hashCode();
    }

    //-----------------------------------------------------------------------
    /**
     * Compares this mutable to another in ascending order.
     *
     * @param other  the other mutable to compare to, not null
     * @return negative if this is less, zero if equal, positive if greater
     *  where false is less than true
     */
    @Override
    public int compareTo(final MutableBoolean other) {
        return BooleanUtils.compare(this.volatileValue, other.booleanValue());
    }

    //-----------------------------------------------------------------------
    /**
     * Returns the String value of this mutable.
     *
     * @return the mutable value as a string
     */
    @Override
    public String toString() {
        return String.valueOf(volatileValue);
    }
}
