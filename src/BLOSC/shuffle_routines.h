/* qs - Quick Serialization of R Objects
 Copyright (C) 2019-present Travers Ching
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <https://www.gnu.org/licenses/>.
 
 You can contact the author at:
 https://github.com/traversc/qs
 */

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

static inline void shuffle_generic_inline(const uint64_t type_size,
                                          const uint64_t vectorizable_elements, const uint64_t blocksize,
                                          const uint8_t* const _src, uint8_t* const _dest) {
  uint64_t i, j;
  const uint64_t neblock_quot = blocksize / type_size;
  
  /* Non-optimized shuffle */
  for (j = 0; j < type_size; j++) {
    for (i = vectorizable_elements; i < neblock_quot; i++) {
      _dest[j*neblock_quot+i] = _src[i*type_size+j];
    }
  }
}

#if defined (__AVX2__)

static void shuffle8_avx2(uint8_t* const dest, const uint8_t* const src,
                          const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 8;
  uint64_t j;
  int k, l;
  __m256i ymm0[8], ymm1[8];
  
  for (j = 0; j < vectorizable_elements; j += sizeof(__m256i)) {
    /* Fetch 32 elements (256 bytes) then transpose bytes. */
    for (k = 0; k < 8; k++) {
      ymm0[k] = _mm256_loadu_si256((__m256i*)(src + (j * bytesoftype) + (k * sizeof(__m256i))));
      ymm1[k] = _mm256_shuffle_epi32(ymm0[k], 0x4e);
      ymm1[k] = _mm256_unpacklo_epi8(ymm0[k], ymm1[k]);
    }
    /* Transpose words */
    for (k = 0, l = 0; k < 4; k++, l +=2) {
      ymm0[k*2] = _mm256_unpacklo_epi16(ymm1[l], ymm1[l+1]);
      ymm0[k*2+1] = _mm256_unpackhi_epi16(ymm1[l], ymm1[l+1]);
    }
    /* Transpose double words */
    for (k = 0, l = 0; k < 4; k++, l++) {
      if (k == 2) l += 2;
      ymm1[k*2] = _mm256_unpacklo_epi32(ymm0[l], ymm0[l+2]);
      ymm1[k*2+1] = _mm256_unpackhi_epi32(ymm0[l], ymm0[l+2]);
    }
    /* Transpose quad words */
    for (k = 0; k < 4; k++) {
      ymm0[k*2] = _mm256_unpacklo_epi64(ymm1[k], ymm1[k+4]);
      ymm0[k*2+1] = _mm256_unpackhi_epi64(ymm1[k], ymm1[k+4]);
    }
    for(k = 0; k < 8; k++) {
      ymm1[k] = _mm256_permute4x64_epi64(ymm0[k], 0x72);
      ymm0[k] = _mm256_permute4x64_epi64(ymm0[k], 0xD8);
      ymm0[k] = _mm256_unpacklo_epi16(ymm0[k], ymm1[k]);
    }
    /* Store the result vectors */
    uint8_t* const dest_for_jth_element = dest + j;
    for (k = 0; k < 8; k++) {
      _mm256_storeu_si256((__m256i*)(dest_for_jth_element + (k * total_elements)), ymm0[k]);
    }
  }
}

static void shuffle4_avx2(uint8_t* const dest, const uint8_t* const src,
                          const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 4;
  uint64_t i;
  int j;
  __m256i ymm0[4], ymm1[4];
  
  /* Create the shuffle mask.
   NOTE: The XMM/YMM 'set' intrinsics require the arguments to be ordered from
   most to least significant (i.e., their order is reversed when compared to
   loading the mask from an array). */
  const __m256i mask = _mm256_set_epi32(
    0x07, 0x03, 0x06, 0x02, 0x05, 0x01, 0x04, 0x00);
  
  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Fetch 32 elements (128 bytes) then transpose bytes and words. */
    for (j = 0; j < 4; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src + (i * bytesoftype) + (j * sizeof(__m256i))));
      ymm1[j] = _mm256_shuffle_epi32(ymm0[j], 0xd8);
      ymm0[j] = _mm256_shuffle_epi32(ymm0[j], 0x8d);
      ymm0[j] = _mm256_unpacklo_epi8(ymm1[j], ymm0[j]);
      ymm1[j] = _mm256_shuffle_epi32(ymm0[j], 0x04e);
      ymm0[j] = _mm256_unpacklo_epi16(ymm0[j], ymm1[j]);
    }
    /* Transpose double words */
    for (j = 0; j < 2; j++) {
      ymm1[j*2] = _mm256_unpacklo_epi32(ymm0[j*2], ymm0[j*2+1]);
      ymm1[j*2+1] = _mm256_unpackhi_epi32(ymm0[j*2], ymm0[j*2+1]);
    }
    /* Transpose quad words */
    for (j = 0; j < 2; j++) {
      ymm0[j*2] = _mm256_unpacklo_epi64(ymm1[j], ymm1[j+2]);
      ymm0[j*2+1] = _mm256_unpackhi_epi64(ymm1[j], ymm1[j+2]);
    }
    for (j = 0; j < 4; j++) {
      ymm0[j] = _mm256_permutevar8x32_epi32(ymm0[j], mask);
    }
    /* Store the result vectors */
    uint8_t* const dest_for_ith_element = dest + i;
    for (j = 0; j < 4; j++) {
      _mm256_storeu_si256((__m256i*)(dest_for_ith_element + (j * total_elements)), ymm0[j]);
    }
  }
}

#elif defined(__SSE2__)

static void
  shuffle8_sse2(uint8_t* const dest, const uint8_t* const src,
                const uint64_t vectorizable_elements, const uint64_t total_elements) {
    static const uint64_t bytesoftype = 8;
    uint64_t j;
    int k, l;
    uint8_t* dest_for_jth_element;
    __m128i xmm0[8], xmm1[8];
    
    for (j = 0; j < vectorizable_elements; j += sizeof(__m128i)) {
      /* Fetch 16 elements (128 bytes) then transpose bytes. */
      for (k = 0; k < 8; k++) {
        xmm0[k] = _mm_loadu_si128((__m128i*)(src + (j * bytesoftype) + (k * sizeof(__m128i))));
        xmm1[k] = _mm_shuffle_epi32(xmm0[k], 0x4e);
        xmm1[k] = _mm_unpacklo_epi8(xmm0[k], xmm1[k]);
      }
      /* Transpose words */
      for (k = 0, l = 0; k < 4; k++, l +=2) {
        xmm0[k*2] = _mm_unpacklo_epi16(xmm1[l], xmm1[l+1]);
        xmm0[k*2+1] = _mm_unpackhi_epi16(xmm1[l], xmm1[l+1]);
      }
      /* Transpose double words */
      for (k = 0, l = 0; k < 4; k++, l++) {
        if (k == 2) l += 2;
        xmm1[k*2] = _mm_unpacklo_epi32(xmm0[l], xmm0[l+2]);
        xmm1[k*2+1] = _mm_unpackhi_epi32(xmm0[l], xmm0[l+2]);
      }
      /* Transpose quad words */
      for (k = 0; k < 4; k++) {
        xmm0[k*2] = _mm_unpacklo_epi64(xmm1[k], xmm1[k+4]);
        xmm0[k*2+1] = _mm_unpackhi_epi64(xmm1[k], xmm1[k+4]);
      }
      /* Store the result vectors */
      dest_for_jth_element = dest + j;
      for (k = 0; k < 8; k++) {
        _mm_storeu_si128((__m128i*)(dest_for_jth_element + (k * total_elements)), xmm0[k]);
      }
    }
  }

static void shuffle4_sse2(uint8_t* const dest, const uint8_t* const src,
                          const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 4;
  uint64_t i;
  int j;
  uint8_t* dest_for_ith_element;
  __m128i xmm0[4], xmm1[4];
  
  for (i = 0; i < vectorizable_elements; i += sizeof(__m128i)) {
    /* Fetch 16 elements (64 bytes) then transpose bytes and words. */
    for (j = 0; j < 4; j++) {
      xmm0[j] = _mm_loadu_si128((__m128i*)(src + (i * bytesoftype) + (j * sizeof(__m128i))));
      xmm1[j] = _mm_shuffle_epi32(xmm0[j], 0xd8);
      xmm0[j] = _mm_shuffle_epi32(xmm0[j], 0x8d);
      xmm0[j] = _mm_unpacklo_epi8(xmm1[j], xmm0[j]);
      xmm1[j] = _mm_shuffle_epi32(xmm0[j], 0x04e);
      xmm0[j] = _mm_unpacklo_epi16(xmm0[j], xmm1[j]);
    }
    /* Transpose double words */
    for (j = 0; j < 2; j++) {
      xmm1[j*2] = _mm_unpacklo_epi32(xmm0[j*2], xmm0[j*2+1]);
      xmm1[j*2+1] = _mm_unpackhi_epi32(xmm0[j*2], xmm0[j*2+1]);
    }
    /* Transpose quad words */
    for (j = 0; j < 2; j++) {
      xmm0[j*2] = _mm_unpacklo_epi64(xmm1[j], xmm1[j+2]);
      xmm0[j*2+1] = _mm_unpackhi_epi64(xmm1[j], xmm1[j+2]);
    }
    /* Store the result vectors */
    dest_for_ith_element = dest + i;
    for (j = 0; j < 4; j++) {
      _mm_storeu_si128((__m128i*)(dest_for_ith_element + (j * total_elements)), xmm0[j]);
    }
  }
}

#else
// if neither supported, no other functions needed
#endif

// shuffle dispatcher
static void blosc_shuffle(const uint8_t * const src, uint8_t * const dest, const uint64_t blocksize, const uint64_t bytesoftype) {
  #if defined (__AVX2__)
  uint64_t total_elements = blocksize / bytesoftype;
  uint64_t vectorized_chunk_size = bytesoftype * sizeof(__m256i);
  uint64_t vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
  uint64_t vectorizable_elements = vectorizable_bytes / bytesoftype;
  switch(bytesoftype) {
  case 4:
    shuffle4_avx2(dest, src, vectorizable_elements, total_elements);
    break;
  case 8:
    shuffle8_avx2(dest, src, vectorizable_elements, total_elements);
    break;
  }
  if(blocksize != vectorizable_bytes) shuffle_generic_inline(bytesoftype, vectorizable_elements, blocksize, src, dest);
#elif defined(__SSE2__)
  uint64_t total_elements = blocksize / bytesoftype;
  uint64_t vectorized_chunk_size = bytesoftype * sizeof(__m128i);
  uint64_t vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
  uint64_t vectorizable_elements = vectorizable_bytes / bytesoftype;
  switch(bytesoftype) {
  case 4:
    shuffle4_sse2(dest, src, vectorizable_elements, total_elements);
    break;
  case 8:
    shuffle8_sse2(dest, src, vectorizable_elements, total_elements);
    break;
  }
  if(blocksize != vectorizable_bytes) shuffle_generic_inline(bytesoftype, vectorizable_elements, blocksize, src, dest);
#else
  shuffle_generic_inline(bytesoftype, 0, blocksize, src, dest);
#endif
}
