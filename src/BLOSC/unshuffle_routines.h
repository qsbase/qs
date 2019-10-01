/* The following shuffle routines were adapted from the Blosc meta-compression library 
 */

/*  Blosc - Blocked Shuffling and Compression Library
 Author: Francesc Alted <francesc@blosc.org>
 See src/BLOSC_shuffle/BLOSC for details about copyright and rights to use. 
 */


#if defined (__AVX2__)
#include "immintrin.h"
#elif defined(__SSE2__)
#include "emmintrin.h"
#else
//no other includes necessary
#endif

static inline void unshuffle_generic_inline(const uint64_t type_size,
                                     const uint64_t vectorizable_elements, const uint64_t blocksize,
                                     const uint8_t* const _src, uint8_t* const _dest) {
  uint64_t i, j;
  
  /* Calculate the number of elements in the block. */
  const uint64_t neblock_quot = blocksize / type_size;
  
  /* Non-optimized unshuffle */
  for (i = vectorizable_elements; i < neblock_quot; i++) {
    for (j = 0; j < type_size; j++) {
      _dest[i*type_size+j] = _src[j*neblock_quot+i];
    }
  }
}

#if defined (__AVX2__)

static void unshuffle4_avx2(uint8_t* const dest, const uint8_t* const src,
                            const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 4;
  uint64_t i;
  int j;
  __m256i ymm0[4], ymm1[4];
  
  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Load 32 elements (128 bytes) into 4 YMM registers. */
    const uint8_t* const src_for_ith_element = src + i;
    for (j = 0; j < 4; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src_for_ith_element + (j * total_elements)));
    }
    /* Shuffle bytes */
    for (j = 0; j < 2; j++) {
      /* Compute the low 64 bytes */
      ymm1[j] = _mm256_unpacklo_epi8(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 64 bytes */
      ymm1[2+j] = _mm256_unpackhi_epi8(ymm0[j*2], ymm0[j*2+1]);
    }
    /* Shuffle 2-byte words */
    for (j = 0; j < 2; j++) {
      /* Compute the low 64 bytes */
      ymm0[j] = _mm256_unpacklo_epi16(ymm1[j*2], ymm1[j*2+1]);
      /* Compute the hi 64 bytes */
      ymm0[2+j] = _mm256_unpackhi_epi16(ymm1[j*2], ymm1[j*2+1]);
    }
    ymm1[0] = _mm256_permute2x128_si256(ymm0[0], ymm0[2], 0x20);
    ymm1[1] = _mm256_permute2x128_si256(ymm0[1], ymm0[3], 0x20);
    ymm1[2] = _mm256_permute2x128_si256(ymm0[0], ymm0[2], 0x31);
    ymm1[3] = _mm256_permute2x128_si256(ymm0[1], ymm0[3], 0x31);
    
    /* Store the result vectors in proper order */
    for (j = 0; j < 4; j++) {
      _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (j * sizeof(__m256i))), ymm1[j]);
    }
  }
}

static void unshuffle8_avx2(uint8_t* const dest, const uint8_t* const src,
                  const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 8;
  uint64_t i;
  int j;
  __m256i ymm0[8], ymm1[8];
  
  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Fetch 32 elements (256 bytes) into 8 YMM registers. */
    const uint8_t* const src_for_ith_element = src + i;
    for (j = 0; j < 8; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src_for_ith_element + (j * total_elements)));
    }
    /* Shuffle bytes */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      ymm1[j] = _mm256_unpacklo_epi8(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm1[4+j] = _mm256_unpackhi_epi8(ymm0[j*2], ymm0[j*2+1]);
    }
    /* Shuffle words */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      ymm0[j] = _mm256_unpacklo_epi16(ymm1[j*2], ymm1[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm0[4+j] = _mm256_unpackhi_epi16(ymm1[j*2], ymm1[j*2+1]);
    }
    for (j = 0; j < 8; j++) {
      ymm0[j] = _mm256_permute4x64_epi64(ymm0[j], 0xd8);
    }
    
    /* Shuffle 4-byte dwords */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      ymm1[j] = _mm256_unpacklo_epi32(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm1[4+j] = _mm256_unpackhi_epi32(ymm0[j*2], ymm0[j*2+1]);
    }
    
    /* Store the result vectors in proper order */
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (0 * sizeof(__m256i))), ymm1[0]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (1 * sizeof(__m256i))), ymm1[2]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (2 * sizeof(__m256i))), ymm1[1]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (3 * sizeof(__m256i))), ymm1[3]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (4 * sizeof(__m256i))), ymm1[4]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (5 * sizeof(__m256i))), ymm1[6]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (6 * sizeof(__m256i))), ymm1[5]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (7 * sizeof(__m256i))), ymm1[7]);
  }
}



#elif defined(__SSE2__)

static void unshuffle4_sse2(uint8_t* const dest, const uint8_t* const src,
                  const uint64_t vectorizable_elements, const uint64_t total_elements) {
    static const uint64_t bytesoftype = 4;
    uint64_t i;
    int j;
    __m128i xmm0[4], xmm1[4];
    
    for (i = 0; i < vectorizable_elements; i += sizeof(__m128i)) {
      /* Load 16 elements (64 bytes) into 4 XMM registers. */
      const uint8_t* const src_for_ith_element = src + i;
      for (j = 0; j < 4; j++) {
        xmm0[j] = _mm_loadu_si128((__m128i*)(src_for_ith_element + (j * total_elements)));
      }
      /* Shuffle bytes */
      for (j = 0; j < 2; j++) {
        /* Compute the low 32 bytes */
        xmm1[j] = _mm_unpacklo_epi8(xmm0[j*2], xmm0[j*2+1]);
        /* Compute the hi 32 bytes */
        xmm1[2+j] = _mm_unpackhi_epi8(xmm0[j*2], xmm0[j*2+1]);
      }
      /* Shuffle 2-byte words */
      for (j = 0; j < 2; j++) {
        /* Compute the low 32 bytes */
        xmm0[j] = _mm_unpacklo_epi16(xmm1[j*2], xmm1[j*2+1]);
        /* Compute the hi 32 bytes */
        xmm0[2+j] = _mm_unpackhi_epi16(xmm1[j*2], xmm1[j*2+1]);
      }
      /* Store the result vectors in proper order */
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (0 * sizeof(__m128i))), xmm0[0]);
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (1 * sizeof(__m128i))), xmm0[2]);
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (2 * sizeof(__m128i))), xmm0[1]);
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (3 * sizeof(__m128i))), xmm0[3]);
    }
  }

/* Routine optimized for unshuffling a buffer for a type size of 8 bytes. */
static void unshuffle8_sse2(uint8_t* const dest, const uint8_t* const src,
                  const uint64_t vectorizable_elements, const uint64_t total_elements) {
    static const uint64_t bytesoftype = 8;
    uint64_t i;
    int j;
    __m128i xmm0[8], xmm1[8];
    
    for (i = 0; i < vectorizable_elements; i += sizeof(__m128i)) {
      /* Load 16 elements (128 bytes) into 8 XMM registers. */
      const uint8_t* const src_for_ith_element = src + i;
      for (j = 0; j < 8; j++) {
        xmm0[j] = _mm_loadu_si128((__m128i*)(src_for_ith_element + (j * total_elements)));
      }
      /* Shuffle bytes */
      for (j = 0; j < 4; j++) {
        /* Compute the low 32 bytes */
        xmm1[j] = _mm_unpacklo_epi8(xmm0[j*2], xmm0[j*2+1]);
        /* Compute the hi 32 bytes */
        xmm1[4+j] = _mm_unpackhi_epi8(xmm0[j*2], xmm0[j*2+1]);
      }
      /* Shuffle 2-byte words */
      for (j = 0; j < 4; j++) {
        /* Compute the low 32 bytes */
        xmm0[j] = _mm_unpacklo_epi16(xmm1[j*2], xmm1[j*2+1]);
        /* Compute the hi 32 bytes */
        xmm0[4+j] = _mm_unpackhi_epi16(xmm1[j*2], xmm1[j*2+1]);
      }
      /* Shuffle 4-byte dwords */
      for (j = 0; j < 4; j++) {
        /* Compute the low 32 bytes */
        xmm1[j] = _mm_unpacklo_epi32(xmm0[j*2], xmm0[j*2+1]);
        /* Compute the hi 32 bytes */
        xmm1[4+j] = _mm_unpackhi_epi32(xmm0[j*2], xmm0[j*2+1]);
      }
      /* Store the result vectors in proper order */
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (0 * sizeof(__m128i))), xmm1[0]);
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (1 * sizeof(__m128i))), xmm1[4]);
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (2 * sizeof(__m128i))), xmm1[2]);
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (3 * sizeof(__m128i))), xmm1[6]);
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (4 * sizeof(__m128i))), xmm1[1]);
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (5 * sizeof(__m128i))), xmm1[5]);
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (6 * sizeof(__m128i))), xmm1[3]);
      _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (7 * sizeof(__m128i))), xmm1[7]);
    }
  }

#else
// if neither supported, no other functions needed
#endif

// shuffle dispatcher
static void blosc_unshuffle(const uint8_t * const src, uint8_t * const dest, const uint64_t blocksize, const uint64_t bytesoftype) {
  #if defined (__AVX2__)
  uint64_t total_elements = blocksize / bytesoftype;
  uint64_t vectorized_chunk_size = bytesoftype * sizeof(__m256i);
  uint64_t vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
  uint64_t vectorizable_elements = vectorizable_bytes / bytesoftype;
  switch(bytesoftype) {
  case 4:
    unshuffle4_avx2(dest, src, vectorizable_elements, total_elements);
    break;
  case 8:
    unshuffle8_avx2(dest, src, vectorizable_elements, total_elements);
    break;
  }
  unshuffle_generic_inline(bytesoftype, vectorizable_elements, blocksize, src, dest);
#elif defined(__SSE2__)
  uint64_t total_elements = blocksize / bytesoftype;
  uint64_t vectorized_chunk_size = bytesoftype * sizeof(__m128i);
  uint64_t vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
  uint64_t vectorizable_elements = vectorizable_bytes / bytesoftype;
  switch(bytesoftype) {
  case 4:
    unshuffle4_sse2(dest, src, vectorizable_elements, total_elements);
    break;
  case 8:
    unshuffle8_sse2(dest, src, vectorizable_elements, total_elements);
    break;
  }
  unshuffle_generic_inline(bytesoftype, vectorizable_elements, blocksize, src, dest);
#else
  unshuffle_generic_inline(bytesoftype, 0, blocksize, src, dest);
#endif
}
