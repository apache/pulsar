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
#include <algorithm> // std::swap

#ifdef _MSC_VER
#pragma warning(disable:4146) // unary minus operator applied to unsigned type, result still unsigned
#endif

// Type trait for unsigned integers of at least N bytes
template<unsigned int N>
struct uint_bytes {
    enum {
        is_defined = 0
    };
};

template<class T>
struct defined_uint_bytes {
    enum {
        is_defined = 1
    };
    typedef T type;
};

template<> struct uint_bytes<1> : defined_uint_bytes<uint8_t> {
};
template<> struct uint_bytes<2> : defined_uint_bytes<uint16_t> {
};
template<> struct uint_bytes<3> : defined_uint_bytes<uint32_t> {
};
template<> struct uint_bytes<4> : defined_uint_bytes<uint32_t> {
};
template<> struct uint_bytes<5> : defined_uint_bytes<uint64_t> {
};
template<> struct uint_bytes<6> : defined_uint_bytes<uint64_t> {
};
template<> struct uint_bytes<7> : defined_uint_bytes<uint64_t> {
};
template<> struct uint_bytes<8> : defined_uint_bytes<uint64_t> {
};

// Type trait for unsigned integers of at least N bits
template<unsigned int N>
struct uint_bits : uint_bytes<(N + 7) / 8> {
    enum {
        bits = 8 * sizeof(typename uint_bits::type)
    };
};

// Bit vector of N bits; currently just exposes an unsigned integer
template<unsigned int N>
class bitvector {
    typedef typename uint_bits<N>::type type;
    type value;
 public:
    bitvector() {
    }
    bitvector(type value)
            : value(value) {
    }
    operator type&() {
        return value;
    }
    operator type() const {
        return value;
    }
};

// Bit matrix of M columns by N rows
template<unsigned int M, unsigned int N = M>
class bitmatrix {
    typedef bitvector<M> row;
    row value[N];
 public:
    bitmatrix() {
    }
    explicit bitmatrix(bool b) {
        if (b)
            identity();
        else
            null();
    }
    void null() {
        for (unsigned int i = 0; i < N; ++i)
            value[i] = 0;
    }
    void identity() {
        for (unsigned int i = 0; i < N; ++i)
            value[i] = i < M ? (const row) 1 << i : 0;
    }
    void lower_shift() {
        for (unsigned int i = 0; i < N; ++i)
            value[i] = i > 0 && i <= M ? (const row) 1 << (i - 1) : 0;
    }
    void upper_shift() {
        for (unsigned int i = 0; i < N; ++i)
            value[i] = i + 1 < M ? (const row) 1 << (i + 1) : 0;
    }
    operator bitvector<M>*() {
        return value;
    }
    operator const bitvector<M>*() const {
        return value;
    }
};

/*
 * Multiplies MxN matrix A by N-row vector B in GF(2).
 *
 * For M,N = 3:
 *
 *     | a b c |      | x |       | ax + by + cz |
 * A = | d e f |, B = | y |, AB = | dx + ey + fz |
 *     | g h i |      | z |       | gx + hy + iz |
 *
 * In GF(2), addition corresponds to XOR and multiplication to AND:
 *
 *      | (a & x) ^ (b & y) ^ (c & z) |
 * AB = | (d & x) ^ (e & y) ^ (f & z) |
 *      | (g & x) ^ (h & y) ^ (i & z) |
 *
 * Trading variable names for [row,column] indices:
 *
 * AB = (A[,0] & B[0]) ^ (A[,1] & B[1]) ^ (A[,2] & B[2]) ^ ...
 *
 * Assuming columns are represented as words and rows as bit offsets,
 * all rows of AB can be calculated in parallel:
 *
 * AB = (A[0] & -((B >> 0) & 1) ^ (A[1] & -((B >> 1) & 1) ^ ...
 */
template<unsigned int M, unsigned int N>
bitvector<M> mul(const bitmatrix<M, N>& a, const bitvector<N> b) {
    bitvector<M> result(0);
    for (unsigned int i = 0; i < N; ++i)
        result ^= a[i] & -((b >> i) & 1);
    return result;
}

/*
 * Multiplies MxN matrix A by NxP matrix B in GF(2).
 *
 * For M,N,P = 3:
 *
 *     | a b c |      | j k l |       | (aj + bm + cp) (ak + bn + cq) (al + bo + cr) |
 * A = | d e f |, B = | m n o |, AB = | (dj + em + fp) (dk + en + fq) (dl + eo + fr) |
 *     | g h i |      | p q r |       | (gj + hm + ip) (gk + hn + iq) (gl + ho + ir) |
 */
template<unsigned int M, unsigned int N, unsigned int P>
void mul(bitmatrix<M, P>& result, const bitmatrix<M, N>& a, const bitmatrix<N, P>& b) {
    for (unsigned int i = 0; i < P; i++)
        result[i] = mul(a, b[i]);
}

/*
 * Squares an NxN matrix in GF(2).
 */
template<unsigned int N>
void sqr(bitmatrix<N, N>& result, const bitmatrix<N, N>& a) {
    mul(result, a, a);
}

/*
 * Raises an NxN matrix to the power n in GF(2) by squaring.
 */
template<unsigned int N>
void pow(bitmatrix<N, N>& result, const bitmatrix<N, N>& a, uint64_t n) {
    result.identity();
    if (n > 0) {
        bitmatrix<N, N> square = a;
        bitmatrix<N, N> temp;
        bitmatrix<N, N> *ptemp = &temp, *psquare = &square, *presult = &result;
        for (;;) {
            if (n & 1) {
                mul(*ptemp, *presult, *psquare);
                std::swap(ptemp, presult);
            }
            if (!(n >>= 1))
                break;
            sqr(*ptemp, *psquare);
            std::swap(ptemp, psquare);
        }
        if (presult != &result)
            result = *presult;
    }
}
