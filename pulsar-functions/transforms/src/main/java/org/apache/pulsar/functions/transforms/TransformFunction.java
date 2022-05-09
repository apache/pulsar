package org.apache.pulsar.functions.transforms;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.LinkedList;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

@Slf4j
public class TransformFunction implements Function<GenericObject, Void> {

    LinkedList<Map<String, String>> steps;

    @Override
    public void initialize(Context context) {
        TypeToken<LinkedList<Map<String, String>>> type = new TypeToken<LinkedList<Map<String, String>>>(){};
        steps = new Gson().fromJson(
                (new Gson().toJson(context.getUserConfigValue("steps").get())),
                type.getType());
    }

    @Override
    public Void process(GenericObject genericObject, Context context) throws Exception {
        Record<?> currentRecord = context.getCurrentRecord();
        Schema<?> schema = currentRecord.getSchema();
        Object nativeObject = genericObject.getNativeObject();
        log.info("apply to {} {}", genericObject, nativeObject);
        log.info("record with schema {} version {} {}", schema,
                currentRecord.getMessage().get().getSchemaVersion(),
                currentRecord);

        TransformResult transformResult = new TransformResult(schema, nativeObject);
        for (Map<String, String> step : steps) {
            String type = step.get("type");
            if ("drop-field".equals(type)) {
                transformResult = TransformUtils.dropValueField(step.get("field"), transformResult);
            }
        }
        TransformUtils.sendMessage(context, transformResult);
        return null;
    }
}
