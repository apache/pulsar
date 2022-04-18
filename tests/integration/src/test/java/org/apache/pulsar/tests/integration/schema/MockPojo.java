package org.apache.pulsar.tests.integration.schema;

import java.util.Objects;
import org.jetbrains.annotations.NotNull;

public class MockPojo implements Comparable<MockPojo> {
    String str;
    int num;

    public MockPojo() {}

    public MockPojo(String str, int num) {
        this.str = str;
        this.num = num;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MockPojo mockPojo = (MockPojo) o;
        return num == mockPojo.num && Objects.equals(str, mockPojo.str);
    }

    @Override
    public int hashCode() {
        return Objects.hash(str, num);
    }

    @Override
    public int compareTo(@NotNull MockPojo o) {
        return this.equals(o) ? 0 : 1;
    }
}
