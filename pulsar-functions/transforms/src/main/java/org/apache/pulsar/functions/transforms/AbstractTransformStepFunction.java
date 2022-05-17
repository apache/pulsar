package org.apache.pulsar.functions.transforms;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

@Slf4j
public abstract class AbstractTransformStepFunction implements Function<GenericObject, Void>, TransformStep{

    @Override
    public Void process(GenericObject input, Context context) throws Exception {
        Record<?> currentRecord = context.getCurrentRecord();
        Schema<?> schema = currentRecord.getSchema();
        Object nativeObject = input.getNativeObject();
        if (log.isDebugEnabled()) {
            log.debug("apply to {} {}", input, nativeObject);
            log.debug("record with schema {} version {} {}", schema,
                    currentRecord.getMessage().get().getSchemaVersion(),
                    currentRecord);
        }

        TransformContext transformContext = new TransformContext(context, nativeObject);
        process(transformContext);
        transformContext.send();
        return null;
    }
}
