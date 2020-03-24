package org.apache.pulsar.functions.api.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.examples.pojo.AvroTestObject;


@Slf4j
public class AvroSchemaTestFunction implements Function<AvroTestObject, AvroTestObject> {

    @Override
    public AvroTestObject process(AvroTestObject input, Context context) throws Exception {
        log.info("AvroTestObject - baseValue: {}", input.getBaseValue());
        input.setBaseValue(input.getBaseValue() + 10);
        return input;
    }
}
