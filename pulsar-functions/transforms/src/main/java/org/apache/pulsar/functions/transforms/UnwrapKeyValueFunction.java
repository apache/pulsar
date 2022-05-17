package org.apache.pulsar.functions.transforms;

import org.apache.pulsar.functions.api.Context;

public class UnwrapKeyValueFunction extends AbstractTransformStepFunction {

    boolean unwrapKey;

    public UnwrapKeyValueFunction() {}

    public UnwrapKeyValueFunction(boolean unwrapKey) {
        this.unwrapKey = unwrapKey;
    }

    @Override
    public void initialize(Context context) {
        this.unwrapKey = (Boolean) context.getUserConfigValue("unwrapKey")
                .map(value -> {
                    if (!(value instanceof Boolean)) {
                        throw new IllegalArgumentException("unwrapKey param must be of type Boolean");
                    }
                    return value;
                })
                .orElse(false);
    }

    @Override
    public void process(TransformContext transformContext) throws Exception {
        if (transformContext.getKeySchema() != null) {
            if (unwrapKey) {
                transformContext.setValueSchema(transformContext.getKeySchema());
                transformContext.setValueObject(transformContext.getKeyObject());
            }
            // TODO: can we avoid making the conversion to NATIVE_AVRO ?
            transformContext.setValueModified(true);
            transformContext.setKeySchema(null);
            transformContext.setKeyObject(null);
        }
    }
}
