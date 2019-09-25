
package org.apache.pulsar.schema.compatibility;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.avro.reflect.AvroDefault;

public class Schemas {

    @Data
    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PersonOne{
        int id;
    }

    @Data
    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PersonTwo{
        int id;

        @AvroDefault("\"Tom\"")
        String name;
    }

    @Data
    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PersonThree{
        int id;

        String name;
    }

    @Data
    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PersonFour{
        int id;

        String name;

        int age;
    }
}
