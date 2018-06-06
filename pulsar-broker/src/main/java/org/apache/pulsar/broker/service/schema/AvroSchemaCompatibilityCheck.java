package org.apache.pulsar.broker.service.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.pulsar.common.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Arrays;

public class AvroSchemaCompatibilityCheck implements SchemaCompatibilityCheck {

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.AVRO;
    }

    @Override
    public boolean isCompatible(SchemaData from, SchemaData to) {

        Schema.Parser parser = new Schema.Parser();
        Schema fromSchema = parser.parse(new String(from.getData()));
        Schema toSchema =  parser.parse(new String(to.getData()));

        SchemaValidator schemaValidator = createSchemaValidator(CompatibilityStrategy.BACKWARD, true);
        try {
            schemaValidator.validate(toSchema, Arrays.asList(fromSchema));
        } catch (SchemaValidationException e) {
            return false;
        }
        return true;
    }

    private enum CompatibilityStrategy {
        BACKWARD,
        FORWARD,
        FULL
    }

    private static SchemaValidator createSchemaValidator(CompatibilityStrategy compatibilityStrategy,
                                                  boolean onlyLatestValidator) {
        final SchemaValidatorBuilder validatorBuilder = new SchemaValidatorBuilder();
        switch (compatibilityStrategy) {
            case BACKWARD:
                return createLatestOrAllValidator(validatorBuilder.canReadStrategy(), onlyLatestValidator);
            case FORWARD:
                return createLatestOrAllValidator(validatorBuilder.canBeReadStrategy(), onlyLatestValidator);
            default:
                return createLatestOrAllValidator(validatorBuilder.mutualReadStrategy(), onlyLatestValidator);
        }
    }

    private static SchemaValidator createLatestOrAllValidator(SchemaValidatorBuilder validatorBuilder, boolean onlyLatest) {
        return onlyLatest ? validatorBuilder.validateLatest() : validatorBuilder.validateAll();
    }
}
