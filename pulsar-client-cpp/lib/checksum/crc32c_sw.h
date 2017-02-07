#ifndef LOGGING_CRC32C_SW_H__
#define LOGGING_CRC32C_SW_H__

#include <stdint.h>

uint32_t crc32c_sw(uint32_t crc, const void* data, int length);

#endif
