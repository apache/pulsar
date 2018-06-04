package org.apache.pulsar.functions.api.examples;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class TestFunction implements Function<Integer, Integer>{
    @Override
    public Integer process(Integer input, Context context) throws Exception {
        return null;
    }
}
