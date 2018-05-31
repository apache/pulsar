package org.apache.pulsar.functions.utils.validation;

import org.apache.pulsar.functions.utils.FunctionConfig;

import java.util.Map;

import static org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.ValidatorParams.ACTUAL_RUNTIME;

public abstract class RuntimeSpecificValidator extends Validator {

    private FunctionConfig.Runtime runtime;

    public RuntimeSpecificValidator(Map<String, Object> params) {
        if (!params.containsKey(ACTUAL_RUNTIME)) {
            throw new IllegalArgumentException("Cannot use RuntimeSpecificValidator without specifying a valid Runtime");
        }
        this.runtime = (FunctionConfig.Runtime) params.get(ACTUAL_RUNTIME);
    }

    @Override
    public void validateField(String name, Object o) {
        switch (this.runtime) {
            case JAVA:
                validateFieldJavaRuntime(name, o);
                break;
            case PYTHON:
                validateFieldJavaRuntime(name, o);
                break;
        }
    }

    public abstract void validateFieldJavaRuntime(String name, Object o);

    public abstract void validateFieldPythonRuntime(String name, Object o);
}
