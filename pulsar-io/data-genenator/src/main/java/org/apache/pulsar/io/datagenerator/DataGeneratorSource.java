package org.apache.pulsar.io.datagenerator;

import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

import java.util.Map;
import java.util.Optional;


public class DataGeneratorSource implements Source<Person> {

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {

    }

    @Override
    public Record<Person> read() throws Exception {
        Thread.sleep(50);
        Fairy fairy = Fairy.create();
        return new Record<Person>() {
            @Override
            public Optional<String> getKey() {
                return Optional.empty();
            }

            @Override
            public Person getValue() {
                return fairy.person();
            }
        };
    }

    @Override
    public void close() throws Exception {

    }
}
