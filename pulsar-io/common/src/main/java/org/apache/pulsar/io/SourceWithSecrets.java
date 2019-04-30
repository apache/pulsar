package org.apache.pulsar.io;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public abstract class SourceWithSecrets implements Source {

    @Override
    public void open(Map map, SourceContext sourceContext) throws Exception {
        Map<String, Object> configs = new HashMap<>(map);

        for (Field field : getSourceConfigClass().getDeclaredFields()) {
            field.setAccessible(true);
            for (Annotation annotation : field.getAnnotations()) {
                if (annotation.annotationType().equals(FieldDoc.class)) {
                    if (((FieldDoc) annotation).sensitive()) {
                        String secret = null;
                        try {
                            secret = sourceContext.getSecret(field.getName());
                        } catch (Exception e) {
                            log.warn("Failed to read secret {}", field.getName(), e);
                            break;
                        }

                        if (secret != null) {
                            configs.put(field.getName(), secret);
                        }
                    }
                }
            }
        }
        openWithSecrets(configs, sourceContext);
    }

    public abstract void openWithSecrets(Map map, SourceContext sourceContext);

    public abstract Class getSourceConfigClass();

}
