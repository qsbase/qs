/*
 * basE91 encoding/decoding routines
 *
 * Copyright (c) 2000-2006 Joachim Henke
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *  - Neither the name of Joachim Henke nor the names of his contributors may
 *    be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

// TC modifications:
// 1: constant variable names are more descriptive
// 2: check that we don't write out of bound
// 3: explicit casts to make C++ compatible

#ifndef BASE91_H
#define BASE91_H

#include <stddef.h>

struct basE91 {
  unsigned long queue = 0;
  unsigned int nbits = 0;
  int val = -1;
};

static const unsigned char basE91_encoder_ring[91] = {
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
	'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
	'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '!', '#', '$',
	'%', '&', '(', ')', '*', '+', ',', '.', '/', ':', ';', '<', '=',
	'>', '?', '@', '[', ']', '^', '_', '`', '{', '|', '}', '~', '"' // wish he used dash instead of double quote =/
};
static const unsigned char basE91_decoder_ring[256] = {
	91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91,
	91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91,
	91, 62, 90, 63, 64, 65, 66, 91, 67, 68, 69, 70, 71, 91, 72, 73,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 74, 75, 76, 77, 78, 79,
	80,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
	15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 81, 91, 82, 83, 84,
	85, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
	41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 86, 87, 88, 89, 91,
	91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91,
	91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91,
	91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91,
	91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91,
	91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91,
	91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91,
	91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91,
	91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91, 91
};

// No need for initialization function, we will use default C++ constructor
// void basE91_init(struct basE91 *b) {
// 	b->queue = 0;
// 	b->nbits = 0;
// 	b->val = -1;
// }

size_t basE91_encode_internal(struct basE91 *b, const void *i, size_t len, void *o, size_t olen)
{
	const unsigned char *ib = reinterpret_cast<const unsigned char*>(i);
	unsigned char *ob = reinterpret_cast<unsigned char*>(o);
	size_t n = 0;

	while (len--) {
		b->queue |= *ib++ << b->nbits;
		b->nbits += 8;
		if (b->nbits > 13) {	/* enough bits in queue */
			unsigned int val = b->queue & 8191;

			if (val > 88) {
				b->queue >>= 13;
				b->nbits -= 13;
			} else {	/* we can take 14 bits */
				val = b->queue & 16383;
				b->queue >>= 14;
				b->nbits -= 14;
			}
			if(n+2 >= olen) throw std::runtime_error("base91_encode: error attempted write outside memory bound");
			ob[n++] = basE91_encoder_ring[val % 91];
			ob[n++] = basE91_encoder_ring[val / 91];
		}
	}

	return n;
}

/* process remaining bits from bit queue; write up to 2 bytes */

size_t basE91_encode_end(struct basE91 *b, void *o, size_t olen)
{
  if(olen < 2) throw std::runtime_error("base91_encode: error attempted write outside memory bound");
	unsigned char *ob = reinterpret_cast<unsigned char*>(o);
	size_t n = 0;

	if (b->nbits) {
		ob[n++] = basE91_encoder_ring[b->queue % 91];
		if (b->nbits > 7 || b->queue > 90)
			ob[n++] = basE91_encoder_ring[b->queue / 91];
	}
	b->queue = 0;
	b->nbits = 0;
	b->val = -1;

	return n;
}

size_t basE91_decode_internal(struct basE91 *b, const void *i, size_t len, void *o, size_t olen)
{
  const unsigned char *ib = reinterpret_cast<const unsigned char*>(i);
  unsigned char *ob = reinterpret_cast<unsigned char*>(o);
	size_t n = 0;
	unsigned int d;

	while (len--) {
		d = basE91_decoder_ring[*ib++];
		if (d == 91)
			continue;	/* ignore non-alphabet chars */
		if (b->val == -1)
			b->val = d;	/* start next value */
		else {
			b->val += d * 91;
			b->queue |= b->val << b->nbits;
			b->nbits += (b->val & 8191) > 88 ? 13 : 14;
			do {
			  if(n+1 >= olen) throw std::runtime_error("base91_decode: error attempted write outside memory bound");
				ob[n++] = b->queue;
				b->queue >>= 8;
				b->nbits -= 8;
			} while (b->nbits > 7);
			b->val = -1;	/* mark value complete */
		}
	}

	return n;
}

/* process remaining bits; write at most 1 byte */

size_t basE91_decode_end(struct basE91 *b, void *o, size_t olen)
{
  if(olen == 0) throw std::runtime_error("base91_decode: error attempted write outside memory bound");
  unsigned char *ob = reinterpret_cast<unsigned char*>(o);
	size_t n = 0;
	if (b->val != -1)
		ob[n++] = b->queue | b->val << b->nbits;
	b->queue = 0;
	b->nbits = 0;
	b->val = -1;

	return n;
}

// calculate bounds so we don't segfault
size_t basE91_encode_bound(size_t len) {
  size_t bound = len / 13 * 16;
  bound       += (len % 13 > 0) ? 16 : 0;
  return bound;
}

size_t basE91_decode_bound(size_t len) {
  size_t bound = len / 16 * 14;
  bound       += (len % 16 > 0) ? 14 : 0;
  return bound;
}


#endif