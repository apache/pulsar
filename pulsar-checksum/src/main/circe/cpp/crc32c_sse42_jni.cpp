/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
#include "com_scurrilous_circe_crc_Sse42Crc32C.h"
#include "../include/crc32c_sse42.hpp"
#include <new>

extern "C"
JNIEXPORT jboolean JNICALL Java_com_scurrilous_circe_crc_Sse42Crc32C_nativeSupported
(JNIEnv *, jclass) {
    return crc32c_initialize();
}

extern "C"
JNIEXPORT jint JNICALL Java_com_scurrilous_circe_crc_Sse42Crc32C_nativeArray
(JNIEnv *env, jclass, jint current, jbyteArray input, jint index, jint length, jlong config) {
    const char *buf = (const char *) env->GetPrimitiveArrayCritical(input, 0);
    jint crc = (jint) crc32c((uint32_t) current, buf + index, (size_t) length, (const chunk_config*) config);
    env->ReleasePrimitiveArrayCritical(input, (void*) buf, 0);
    return crc;
}

extern "C"
JNIEXPORT jint JNICALL Java_com_scurrilous_circe_crc_Sse42Crc32C_nativeDirectBuffer
(JNIEnv *env, jclass, jint current, jobject input, jint offset, jint length, jlong config) {
    const char *address = (const char *) env->GetDirectBufferAddress(input);
    if (!address)
        return 0;
    return (jint) crc32c((uint32_t) current, address + offset, (size_t) length, (const chunk_config*) config);
}

extern "C"
JNIEXPORT jint JNICALL Java_com_scurrilous_circe_crc_Sse42Crc32C_nativeUnsafe
(JNIEnv *, jclass, jint current, jlong address, jlong length, jlong config) {
    return (jint) crc32c((uint32_t) current, (const void *) address, (size_t) length, (const chunk_config*) config);
}

extern "C"
JNIEXPORT jlong JNICALL Java_com_scurrilous_circe_crc_Sse42Crc32C_allocConfig
  (JNIEnv *env, jclass, jintArray chunkWords) {
    chunk_config* configs = 0;
    jsize len = env->GetArrayLength(chunkWords);
    const jint *arr = (const jint *) env->GetPrimitiveArrayCritical(chunkWords, 0);
    if (len < 1 || arr[0] < chunk_config::min_words)
        goto fail;
    for (jsize i = 1; i < len; ++i) {
        if (arr[i] < chunk_config::min_words
            || arr[i] >= arr[i - 1]) // chunk words must be strictly decreasing
            goto fail;
    }
    configs = (chunk_config*) ::operator new[](sizeof(chunk_config) * (size_t) len, std::nothrow);
    if (configs) {
        for (jsize i = len; i > 0; --i) {
            new(&configs[i - 1]) chunk_config((size_t) arr[i - 1], i < len ? &configs[i] : 0);
        }
    }
fail:
    env->ReleasePrimitiveArrayCritical(chunkWords, (void*) arr, 0);
    return (jlong) configs;
}

extern "C"
JNIEXPORT void JNICALL Java_com_scurrilous_circe_crc_Sse42Crc32C_freeConfig
  (JNIEnv *, jclass, jlong config) {
    delete[] (const chunk_config*) config;
}
