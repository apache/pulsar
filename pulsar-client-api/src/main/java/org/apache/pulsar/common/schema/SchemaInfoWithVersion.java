package org.apache.pulsar.common.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.pulsar.client.internal.DefaultImplementation;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@Builder
public class SchemaInfoWithVersion {

    private long version;

    private SchemaInfo schemaInfo;

    @Override
    public String toString(){
        return DefaultImplementation.jsonifySchemaInfoWithVersion(this);
    }
}
