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
#include "int_types.h"

namespace pulsar {

bool crc32c_initialize();

class chunk_config {
   public:
    enum
    {
        min_words = 4
    };

    const size_t words;
    const chunk_config *const next;
    uint32_t shift1[256];
    uint32_t shift2[256];

    chunk_config(size_t words, const chunk_config *next = 0);

    size_t loops() const { return (words - 1) / 3; }

    size_t extra() const { return (words - 1) % 3 + 1; }

   private:
    chunk_config &operator=(const chunk_config &);

    static void make_shift_table(size_t bytes, uint32_t table[256]);
};

uint32_t crc32c(uint32_t init, const void *buf, size_t len, const chunk_config *config);
}  // namespace pulsar
