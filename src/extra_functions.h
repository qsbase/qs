/* qs - Quick Serialization of R Objects
  Copyright (C) 2019-present Travers Ching

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <https://www.gnu.org/licenses/>.

 You can contact the author at:
 https://github.com/qsbase/qs
*/

// These are extra functions that are not directly related to qs serialization

#include "qs_common.h"
#include "ascii_encoding/base85.h"
#include "ascii_encoding/base91.h"

// [[Rcpp::interfaces(r, cpp)]]

// [[Rcpp::export(rng = false)]]
std::string check_SIMD() {
#if defined (__AVX2__)
  return "AVX2";
#elif defined (__SSE2__)
  return "SSE2";
#else
  return "no SIMD";
#endif
}

// [[Rcpp::export(rng = false)]]
int zstd_compress_bound(const int size) {
  return ZSTD_compressBound(size);
}

// [[Rcpp::export(rng = false)]]
int lz4_compress_bound(const int size) {
  return LZ4_compressBound(size);
}

// [[Rcpp::export(rng = false)]]
std::vector<unsigned char> zstd_compress_raw(SEXP const x, const int compress_level) {
  if(compress_level > 22 || compress_level < -50) throw std::runtime_error("compress_level must be an integer between -50 and 22");
  uint64_t xsize = Rf_xlength(x);
  uint64_t zsize = ZSTD_compressBound(xsize);
  char* xdata = reinterpret_cast<char*>(RAW(x));
  std::vector<unsigned char> ret(zsize);
  char* retdata = reinterpret_cast<char*>(ret.data());
  zsize = ZSTD_compress(retdata, zsize, xdata, xsize, compress_level);
  ret.resize(zsize);
  return ret;
}

// [[Rcpp::export(rng = false)]]
RawVector zstd_decompress_raw(SEXP const x) {
  uint64_t zsize = Rf_xlength(x);
  char* xdata = reinterpret_cast<char*>(RAW(x));
  uint64_t retsize = ZSTD_getFrameContentSize(xdata, zsize);
  RawVector ret(retsize);
  char* retdata = reinterpret_cast<char*>(RAW(ret));
  ZSTD_decompress(retdata, retsize, xdata, zsize);
  return ret;
}

// [[Rcpp::export(rng = false)]]
std::vector<unsigned char> lz4_compress_raw(SEXP const x, const int compress_level) {
  if(compress_level < 1) throw std::runtime_error("compress_level must be an integer greater than 1");
  uint64_t xsize = Rf_xlength(x);
  uint64_t zsize = LZ4_compressBound(xsize);
  char* xdata = reinterpret_cast<char*>(RAW(x));
  std::vector<unsigned char> ret(zsize);
  char* retdata = reinterpret_cast<char*>(ret.data());
  zsize = LZ4_compress_fast(xdata, retdata, xsize, zsize, compress_level);
  ret.resize(zsize);
  return ret;
}

// [[Rcpp::export(rng = false)]]
std::vector<unsigned char> lz4_decompress_raw(SEXP const x) {
  int zsize = Rf_xlength(x);
  char* xdata = reinterpret_cast<char*>(RAW(x));
  std::vector<unsigned char> ret(zsize*3/2);

  // char* retdata = reinterpret_cast<char*>(ret.data());
  // int decomp = LZ4_decompress_safe(xdata, retdata, zsize, ret.size());
  int decomp = -1;
  while(ret.size() < INT_MAX) {
    char* retdata = reinterpret_cast<char*>(ret.data());
    decomp = LZ4_decompress_safe(xdata, retdata, zsize, ret.size());
    if(decomp < 0) {
      ret.resize(std::min(ret.size() * 2,  static_cast<size_t>(INT_MAX)));
    } else {
      break;
    }
  }
  if(decomp < 0) throw std::runtime_error("lz4 decompression failed");
  ret.resize(decomp);
  return ret;
}

// [[Rcpp::export(rng = false)]]
std::vector<unsigned char> blosc_shuffle_raw(SEXP const x, int bytesofsize) {
  if(bytesofsize != 4 && bytesofsize != 8) throw std::runtime_error("bytesofsize must be 4 or 8");
  uint64_t blocksize = Rf_xlength(x);
  uint8_t* xdata = reinterpret_cast<uint8_t*>(RAW(x));
  std::vector<uint8_t> xshuf(blocksize);
  blosc_shuffle(xdata, xshuf.data(), blocksize, bytesofsize);
  uint64_t remainder = blocksize % bytesofsize;
  uint64_t vectorizablebytes = blocksize - remainder;
  std::memcpy(xshuf.data() + vectorizablebytes, xdata + vectorizablebytes, remainder);
  return xshuf;
}

// [[Rcpp::export(rng = false)]]
std::vector<unsigned char> blosc_unshuffle_raw(SEXP const x, int bytesofsize) {
  if(bytesofsize != 4 && bytesofsize != 8) throw std::runtime_error("bytesofsize must be 4 or 8");
  uint64_t blocksize = Rf_xlength(x);
  uint8_t* xdata = reinterpret_cast<uint8_t*>(RAW(x));
  std::vector<uint8_t> xshuf(blocksize);
  blosc_unshuffle(xdata, xshuf.data(), blocksize, bytesofsize);
  uint64_t remainder = blocksize % bytesofsize;
  uint64_t vectorizablebytes = blocksize - remainder;
  std::memcpy(xshuf.data() + vectorizablebytes, xdata + vectorizablebytes, remainder);
  return xshuf;
}

// [[Rcpp::export(rng = false)]]
std::string xxhash_raw(SEXP const x) {
  uint64_t xsize = Rf_xlength(x);
  uint8_t* xdata = reinterpret_cast<uint8_t*>(RAW(x));
  xxhash_env xenv = xxhash_env();
  xenv.update(xdata, xsize);
  return std::to_string(xenv.digest());
}

// [[Rcpp::export(rng = false)]]
std::string base85_encode(const RawVector & rawdata) {
  size_t size = Rf_xlength(rawdata);
  uint8_t * data = reinterpret_cast<uint8_t*>(RAW(rawdata));
  size_t size_partial = (size / 4) * 4;
  size_t encoded_size_partial = (size / 4) * 5;
  size_t encoded_size = encoded_size_partial + (size % 4 != 0 ? size % 4 + 1 : 0);
  std::string encoded_string(encoded_size,'\0');
  uint8_t * encoded = reinterpret_cast<uint8_t*>(const_cast<char*>(encoded_string.c_str()));

  size_t dbyte = 0;
  size_t ebyte = 0;
  while(dbyte < size_partial) {
    uint32_t value = 16777216UL*data[dbyte] + 65536UL*data[dbyte+1] + 256UL*data[dbyte+2] + data[dbyte+3];
    encoded[ebyte] = base85_encoder_ring[value / 52200625UL];
    encoded[ebyte+1] = base85_encoder_ring[value / 614125UL % 85];
    encoded[ebyte+2] = base85_encoder_ring[value / 7225UL % 85];
    encoded[ebyte+3] = base85_encoder_ring[value / 85UL % 85];
    encoded[ebyte+4] = base85_encoder_ring[value % 85];
    dbyte += 4;
    ebyte += 5;
  }

  size_t leftover_bytes = size - size_partial;
  if(leftover_bytes == 1) {
    uint32_t value = data[dbyte];
    encoded[ebyte] = base85_encoder_ring[value / 85UL % 85];
    encoded[ebyte+1] = base85_encoder_ring[value % 85];
  } else if(leftover_bytes == 2) {
    uint32_t value = 256UL*data[dbyte] + data[dbyte+1];
    encoded[ebyte] = base85_encoder_ring[value / 7225UL];
    encoded[ebyte+1] = base85_encoder_ring[value / 85UL % 85];
    encoded[ebyte+2] = base85_encoder_ring[value % 85];
  } else if(leftover_bytes == 3) {
    uint32_t value = 65536UL*data[dbyte] + 256UL*data[dbyte+1] + data[dbyte+2];
    encoded[ebyte] = base85_encoder_ring[value / 614125UL % 85];
    encoded[ebyte+1] = base85_encoder_ring[value / 7225UL % 85];
    encoded[ebyte+2] = base85_encoder_ring[value / 85UL % 85];
    encoded[ebyte+3] = base85_encoder_ring[value % 85];
  }
  return encoded_string;
}

// [[Rcpp::export(rng = false)]]
RawVector base85_decode(const std::string & encoded_string) {
  size_t size = encoded_string.size();
  size_t size_partial = (size / 5) * 5;
  size_t leftover_bytes = size - size_partial;
  if(leftover_bytes == 1) throw std::runtime_error("base85_decode: corrupted input data, incorrect input size");
  uint8_t * data = reinterpret_cast<uint8_t*>(const_cast<char*>(encoded_string.data()));
  size_t decoded_size_partial = (size / 5)*4;
  size_t decoded_size = decoded_size_partial + (size % 5 != 0 ? size % 5 - 1 : 0);
  RawVector decoded_vector(decoded_size);
  uint8_t * decoded = reinterpret_cast<uint8_t*>(RAW(decoded_vector));

  size_t dbyte = 0;
  size_t ebyte = 0;
  while(ebyte < size_partial) {
    base85_check_byte(data[ebyte]);
    base85_check_byte(data[ebyte+1]);
    base85_check_byte(data[ebyte+2]);
    base85_check_byte(data[ebyte+3]);
    base85_check_byte(data[ebyte+4]);
    uint64_t value_of = 52200625ULL*base85_decoder_ring[data[ebyte]-32] + 614125ULL*base85_decoder_ring[data[ebyte+1]-32];
    value_of         += 7225ULL*base85_decoder_ring[data[ebyte+2]-32] + 85ULL*base85_decoder_ring[data[ebyte+3]-32];
    value_of         += base85_decoder_ring[data[ebyte+4]-32];

    // is there a better way to detect overflow?
    if(value_of > 4294967296ULL) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
    uint32_t value = static_cast<uint32_t>(value_of);
    decoded[dbyte] = value / 16777216UL;
    decoded[dbyte+1] = value / 65536UL % 256;
    decoded[dbyte+2] = value / 256UL % 256;
    decoded[dbyte+3] = value % 256;
    ebyte += 5;
    dbyte += 4;
  }

  if(leftover_bytes == 2) {
    base85_check_byte(data[ebyte]);
    base85_check_byte(data[ebyte+1]);
    uint32_t value = 85UL*base85_decoder_ring[data[ebyte]-32] + base85_decoder_ring[data[ebyte+1]-32];
    if(value > 256) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
    decoded[dbyte] = value;
  } else if(leftover_bytes == 3) {
    base85_check_byte(data[ebyte]);
    base85_check_byte(data[ebyte+1]);
    base85_check_byte(data[ebyte+2]);
    uint32_t value = 7225UL*base85_decoder_ring[data[ebyte]-32] + 85UL*base85_decoder_ring[data[ebyte+1]-32];
    value         += base85_decoder_ring[data[ebyte+2]-32];
    if(value > 65536) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
    decoded[dbyte] = value / 256UL;
    decoded[dbyte+1] = value % 256;
  } else if(leftover_bytes == 4) {
    base85_check_byte(data[ebyte]);
    base85_check_byte(data[ebyte+1]);
    base85_check_byte(data[ebyte+2]);
    base85_check_byte(data[ebyte+3]);
    uint32_t value = 614125UL*base85_decoder_ring[data[ebyte]-32] + 7225UL*base85_decoder_ring[data[ebyte+1]-32];
    value         += 85UL*base85_decoder_ring[data[ebyte+2]-32] + base85_decoder_ring[data[ebyte+3]-32];
    if(value > 16777216) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
    decoded[dbyte] = value / 65536UL;
    decoded[dbyte+1] = value / 256UL % 256;
    decoded[dbyte+2] = value % 256;
  }
  return decoded_vector;
}

// [[Rcpp::export(rng = false)]]
std::string c_base91_encode(const RawVector & rawdata) {
  basE91 b = basE91();
  size_t size = Rf_xlength(rawdata);
  size_t outsize = basE91_encode_bound(size);
  std::string output(outsize, '\0');
  size_t nb_encoded = basE91_encode_internal(&b, RAW(rawdata), size, const_cast<char*>(output.data()), outsize);
  nb_encoded       += basE91_encode_end(&b, const_cast<char*>(output.data()) + nb_encoded, outsize-nb_encoded);
  output.resize(nb_encoded);
  return output;
}

// [[Rcpp::export(rng = false)]]
RawVector c_base91_decode(const std::string & encoded_string) {
  basE91 b = basE91();
  size_t size = encoded_string.size();
  size_t outsize = basE91_decode_bound(size);
  std::vector<uint8_t> output(outsize);
  size_t nb_decoded = basE91_decode_internal(&b, encoded_string.data(), size, output.data(), outsize);
  nb_decoded       += basE91_decode_end(&b, output.data() + nb_decoded, outsize-nb_decoded);
  output.resize(nb_decoded);
  return RawVector(output.begin(), output.end());
}


