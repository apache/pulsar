package org.apache.pulsar.functions.transforms;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

@Slf4j
public class TransformFunction implements Function<GenericObject, Void> {

    private final List<TransformStep> steps = new ArrayList<>();
    private final Gson gson = new Gson();

    @Override
    public void initialize(Context context) {
        Object config = context.getUserConfigValue("steps")
                .orElseThrow(() -> new IllegalArgumentException("missing required 'steps' parameter"));
        LinkedList<Map<String, String>> stepsConfig;
        try {
            TypeToken<LinkedList<Map<String, String>>> typeToken = new TypeToken<LinkedList<Map<String, String>>>(){};
            stepsConfig = gson.fromJson((gson.toJson(config)), typeToken.getType());
        } catch (Exception e) {
            throw new IllegalArgumentException("could not parse configuration", e);
        }
        for (Map<String, String> step : stepsConfig) {
            String type = step.get("type");
            if ("drop-fields".equals(type)) {
                steps.add(newRemoveFieldFunction(step));
            } else {
                throw new IllegalArgumentException("invalid step type: " + type);
            }
        }

    }

    @Override
    public Void process(GenericObject genericObject, Context context) throws Exception {
        Record<?> currentRecord = context.getCurrentRecord();
        Schema<?> schema = currentRecord.getSchema();
        Object nativeObject = genericObject.getNativeObject();
        if (log.isDebugEnabled()) {
            log.debug("apply to {} {}", genericObject, nativeObject);
            log.debug("record with schema {} version {} {}", schema,
                    currentRecord.getMessage().get().getSchemaVersion(),
                    currentRecord);
        }

        TransformContext transformContext = new TransformContext(context, nativeObject);
        for (TransformStep step : steps) {
            step.process(transformContext);
        }
        transformContext.send();
        return null;
    }

    public static RemoveFieldFunction newRemoveFieldFunction(Map<String, String> step) {
        String fields = step.get("fields");
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("missing required 'fields' parameter");
        }
        List<String> fieldList = Arrays.asList(fields.split(","));
        String part = step.get("part");
        if (part == null) {
            return new RemoveFieldFunction(fieldList, fieldList);
        } else if (part.equals("key")) {
            return new RemoveFieldFunction(fieldList, new ArrayList<>());
        } else if (part.equals("value")) {
            return new RemoveFieldFunction(new ArrayList<>(), fieldList);
        } else {
            throw new IllegalArgumentException("invalid 'part' parameter: " + part);
        }
    }
}
