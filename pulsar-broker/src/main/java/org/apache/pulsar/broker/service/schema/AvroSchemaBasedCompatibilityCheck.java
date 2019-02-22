package org.apache.pulsar.broker.service.schema;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.pulsar.common.schema.SchemaData;

/**
 * The abstract implementation of {@link SchemaCompatibilityCheck} using Avro Schema.
 */
abstract class AvroSchemaBasedCompatibilityCheck implements SchemaCompatibilityCheck {

    @Override
    public boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        Schema.Parser fromParser = new Schema.Parser();
        Schema fromSchema = fromParser.parse(new String(from.getData(), UTF_8));
        Schema.Parser toParser = new Schema.Parser();
        Schema toSchema =  toParser.parse(new String(to.getData(), UTF_8));

        SchemaValidator schemaValidator = createSchemaValidator(strategy, true);
        try {
            schemaValidator.validate(toSchema, Arrays.asList(fromSchema));
        } catch (SchemaValidationException e) {
            return false;
        }
        return true;
    }

    static SchemaValidator createSchemaValidator(SchemaCompatibilityStrategy compatibilityStrategy,
                                                 boolean onlyLatestValidator) {
        final SchemaValidatorBuilder validatorBuilder = new SchemaValidatorBuilder();
        switch (compatibilityStrategy) {
            case BACKWARD:
                return createLatestOrAllValidator(validatorBuilder.canReadStrategy(), onlyLatestValidator);
            case FORWARD:
                return createLatestOrAllValidator(validatorBuilder.canBeReadStrategy(), onlyLatestValidator);
            case FULL:
                return createLatestOrAllValidator(validatorBuilder.mutualReadStrategy(), onlyLatestValidator);
            default:
                return NeverSchemaValidator.INSTANCE;
        }
    }

    static SchemaValidator createLatestOrAllValidator(SchemaValidatorBuilder validatorBuilder, boolean onlyLatest) {
        return onlyLatest ? validatorBuilder.validateLatest() : validatorBuilder.validateAll();
    }
}
