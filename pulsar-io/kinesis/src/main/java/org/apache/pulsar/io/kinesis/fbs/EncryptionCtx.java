// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.pulsar.io.kinesis.fbs;

import java.nio.*;
import java.lang.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class EncryptionCtx extends Table {
  public static EncryptionCtx getRootAsEncryptionCtx(ByteBuffer _bb) { return getRootAsEncryptionCtx(_bb, new EncryptionCtx()); }
  public static EncryptionCtx getRootAsEncryptionCtx(ByteBuffer _bb, EncryptionCtx obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public EncryptionCtx __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public EncryptionKey keys(int j) { return keys(new EncryptionKey(), j); }
  public EncryptionKey keys(EncryptionKey obj, int j) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int keysLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public byte param(int j) { int o = __offset(6); return o != 0 ? bb.get(__vector(o) + j * 1) : 0; }
  public int paramLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer paramAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public ByteBuffer paramInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 1); }
  public String algo() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer algoAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public ByteBuffer algoInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 8, 1); }
  public byte compressionType() { int o = __offset(10); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public int uncompressedMessageSize() { int o = __offset(12); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int batchSize() { int o = __offset(14); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public boolean isBatchMessage() { int o = __offset(16); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }

  public static int createEncryptionCtx(FlatBufferBuilder builder,
      int keysOffset,
      int paramOffset,
      int algoOffset,
      byte compressionType,
      int uncompressedMessageSize,
      int batchSize,
      boolean isBatchMessage) {
    builder.startObject(7);
    EncryptionCtx.addBatchSize(builder, batchSize);
    EncryptionCtx.addUncompressedMessageSize(builder, uncompressedMessageSize);
    EncryptionCtx.addAlgo(builder, algoOffset);
    EncryptionCtx.addParam(builder, paramOffset);
    EncryptionCtx.addKeys(builder, keysOffset);
    EncryptionCtx.addIsBatchMessage(builder, isBatchMessage);
    EncryptionCtx.addCompressionType(builder, compressionType);
    return EncryptionCtx.endEncryptionCtx(builder);
  }

  public static void startEncryptionCtx(FlatBufferBuilder builder) { builder.startObject(7); }
  public static void addKeys(FlatBufferBuilder builder, int keysOffset) { builder.addOffset(0, keysOffset, 0); }
  public static int createKeysVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startKeysVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addParam(FlatBufferBuilder builder, int paramOffset) { builder.addOffset(1, paramOffset, 0); }
  public static int createParamVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startParamVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static void addAlgo(FlatBufferBuilder builder, int algoOffset) { builder.addOffset(2, algoOffset, 0); }
  public static void addCompressionType(FlatBufferBuilder builder, byte compressionType) { builder.addByte(3, compressionType, 0); }
  public static void addUncompressedMessageSize(FlatBufferBuilder builder, int uncompressedMessageSize) { builder.addInt(4, uncompressedMessageSize, 0); }
  public static void addBatchSize(FlatBufferBuilder builder, int batchSize) { builder.addInt(5, batchSize, 0); }
  public static void addIsBatchMessage(FlatBufferBuilder builder, boolean isBatchMessage) { builder.addBoolean(6, isBatchMessage, false); }
  public static int endEncryptionCtx(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

