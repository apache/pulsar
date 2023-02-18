package org.apache.bookkeeper.mledger;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.lang.StringUtils;

@Data
@AllArgsConstructor
@ToString
public class MetadataCompressionConfig {
    MLDataFormats.CompressionType compressionType;
    int compressThreshold;

    public MetadataCompressionConfig(String compressionType) throws IllegalArgumentException {
        this(compressionType, 0);
    }

    public MetadataCompressionConfig(String compressionType, int compressThreshold) throws IllegalArgumentException {
        this.compressionType = parseCompressionType(compressionType);
        this.compressThreshold = compressThreshold;
    }

    public static MetadataCompressionConfig noCompression =
            new MetadataCompressionConfig(MLDataFormats.CompressionType.NONE, 0);

    private MLDataFormats.CompressionType parseCompressionType(String value) throws IllegalArgumentException {
        if (StringUtils.isEmpty(value)) {
            return MLDataFormats.CompressionType.NONE;
        }

        MLDataFormats.CompressionType compressionType;
        compressionType = MLDataFormats.CompressionType.valueOf(value);

        return compressionType;
    }
}
