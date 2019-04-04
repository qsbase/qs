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

#ifndef QS_COMMON_H
#define QS_COMMON_H

#include <Rcpp.h>

#include <fstream>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <memory>
#include <array>
#include <cstddef>
#include <type_traits>
#include <utility>
#include <string>
#include <vector>
#include <climits>
#include <limits>

#include <atomic>
#include <thread>
#include <condition_variable>

#include <R.h>
#include <Rinternals.h>
#include <Rversion.h>

#include "RApiSerializeAPI.h"
#include "zstd.h"
#include "lz4.h"
#include "lz4hc.h"
#include "BLOSC/shuffle_routines.h"
#include "BLOSC/unshuffle_routines.h"

#include <R_ext/Rdynload.h>

using namespace Rcpp;

////////////////////////////////////////////////////////////////
// alt rep string class
////////////////////////////////////////////////////////////////

// load alt-rep header -- see altrepisode package by Romain Francois
#if R_VERSION < R_Version(3, 6, 0)
#define class klass
extern "C" {
#include <R_ext/Altrep.h>
}
#undef class
#else
#include <R_ext/Altrep.h>
#endif


struct stdvec_data {
  std::vector<std::string> strings;
  std::vector<unsigned char> encodings;
  R_xlen_t vec_size; 
  stdvec_data(uint64_t N) {
    strings = std::vector<std::string>(N);
    encodings = std::vector<unsigned char>(N);
    vec_size = N;
  }
};

// instead of defining a set of free functions, we structure them
// together in a struct
struct stdvec_string {
  static R_altrep_class_t class_t;
  static SEXP Make(stdvec_data* data, bool owner){
    SEXP xp = PROTECT(R_MakeExternalPtr(data, R_NilValue, R_NilValue));
    if (owner) {
      R_RegisterCFinalizerEx(xp, stdvec_string::Finalize, TRUE);
    }
    SEXP res = R_new_altrep(class_t, xp, R_NilValue);
    UNPROTECT(1);
    return res;
  }
  
  // finalizer for the external pointer
  static void Finalize(SEXP xp){
    delete static_cast<stdvec_data*>(R_ExternalPtrAddr(xp));
  }
  
  // get the std::vector<string>* from the altrep object `x`
  static stdvec_data* Ptr(SEXP vec) {
    return static_cast<stdvec_data*>(R_ExternalPtrAddr(R_altrep_data1(vec)));
  }
  
  // same, but as a reference, for convenience
  static stdvec_data& Get(SEXP vec) {
    return *static_cast<stdvec_data*>(R_ExternalPtrAddr(R_altrep_data1(vec)));
  }
  
  // ALTREP methods -------------------
  
  // The length of the object
  static R_xlen_t Length(SEXP vec){
    return Get(vec).vec_size;
  }
  
  // What gets printed when .Internal(inspect()) is used
  static Rboolean Inspect(SEXP x, int pre, int deep, int pvec, void (*inspect_subtree)(SEXP, int, int, int)){
    Rprintf("qs alt-rep stdvec_string (len=%d, ptr=%p)\n", Length(x), Ptr(x));
    return TRUE;
  }
  
  // ALTVEC methods ------------------
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return std::move(data2);
    }
    R_xlen_t n = Length(vec);
    data2 = PROTECT(Rf_allocVector(STRSXP, n));
    
    auto data1 = Get(vec);
    for (R_xlen_t i = 0; i < n; i++) {
      switch(data1.encodings[i]) {
      case 1:
        SET_STRING_ELT(data2, i, Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_NATIVE) );
        break;
      case 2:
        SET_STRING_ELT(data2, i, Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_UTF8) );
        break;
      case 3:
        SET_STRING_ELT(data2, i, Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_LATIN1) );
        break;
      case 4:
        SET_STRING_ELT(data2, i, Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_BYTES) );
        break;
      default:
        SET_STRING_ELT(data2, i, NA_STRING);
      break;
      }
    }
    
    // free up some memory -- shrink to fit is a non-binding request
    data1.encodings.resize(0);
    data1.encodings.shrink_to_fit();
    data1.strings.resize(0);
    data1.strings.shrink_to_fit();
    
    R_set_altrep_data2(vec, data2);
    UNPROTECT(1);
    return std::move(data2);
  }
  
  // The start of the data, i.e. the underlying double* array from the std::vector<double>
  // This is guaranteed to never allocate (in the R sense)
  static const void* Dataptr_or_null(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 == R_NilValue) return nullptr;
    return STDVEC_DATAPTR(data2);
  }
  
  // same in this case, writeable is ignored
  static void* Dataptr(SEXP vec, Rboolean writeable) {
    return STDVEC_DATAPTR(Materialize(vec));
  }
  
  
  // ALTSTRING methods -----------------
  // the element at the index `i`
  // this does not do bounds checking because that's expensive, so
  // the caller must take care of that
  static SEXP string_Elt(SEXP vec, R_xlen_t i){
    auto data2 = Materialize(vec);
    return STRING_ELT(data2, i);
    // switch(data1.encodings[i]) {
    // case 1:
    //   return Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_NATIVE);
    // case 2:
    //   return Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_UTF8);
    // case 3:
    //   return Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_LATIN1);
    // case 4:
    //   return Rf_mkCharLenCE(data1.strings[i].data(), data1.strings[i].size(), CE_BYTES);
    // default:
    //   return NA_STRING;
    // break;
    // }
  }
  
  // -------- initialize the altrep class with the methods above
  
  static void Init(DllInfo* dll){
    class_t = R_make_altstring_class("stdvec_string", "altrepisode", dll);
    
    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);
    
    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);
    
    // altstring
    R_set_altstring_Elt_method(class_t, string_Elt);
  }
  
};

// static initialization of stdvec_double::class_t
R_altrep_class_t stdvec_string::class_t;

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_stdvec_double(DllInfo* dll){
  stdvec_string::Init(dll);
}

////////////////////////////////////////////////////////////////
// common utility functions and constants
////////////////////////////////////////////////////////////////

bool is_big_endian();

#define BLOCKRESERVE 64
#define NA_STRING_LENGTH 4294967295 // 2^32-1 -- length used to signify NA value
#define MIN_SHUFFLE_ELEMENTS 4

// static const uint64_t SHUFFLE_DONE_VAL = std::numeric_limits<uint64_t>::max();

static const uint64_t BLOCKSIZE = 524288;

static const unsigned char list_header_5 = 0x20; 
static const unsigned char list_header_8 = 0x01;
static const unsigned char list_header_16 = 0x02;
static const unsigned char list_header_32 = 0x03;
static const unsigned char list_header_64 = 0x04;

static const unsigned char numeric_header_5 = 0x40; 
static const unsigned char numeric_header_8 = 0x05;
static const unsigned char numeric_header_16 = 0x06;
static const unsigned char numeric_header_32 = 0x07;
static const unsigned char numeric_header_64 = 0x08;

static const unsigned char integer_header_5 = 0x60; 
static const unsigned char integer_header_8 = 0x09;
static const unsigned char integer_header_16 = 0x0A;
static const unsigned char integer_header_32 = 0x0B;
static const unsigned char integer_header_64 = 0x0C;

static const unsigned char logical_header_5 = 0x80; 
static const unsigned char logical_header_8 = 0x0D;
static const unsigned char logical_header_16 = 0x0E;
static const unsigned char logical_header_32 = 0x0F;
static const unsigned char logical_header_64 = 0x10;

static const unsigned char raw_header_32 = 0x17;
static const unsigned char raw_header_64 = 0x18;

static const unsigned char null_header = 0x00; 

static const unsigned char character_header_5 = 0xA0; 
static const unsigned char character_header_8 = 0x11;
static const unsigned char character_header_16 = 0x12;
static const unsigned char character_header_32 = 0x13;
static const unsigned char character_header_64 = 0x14;

static const unsigned char string_header_NA = 0x0F;
static const unsigned char string_header_5 = 0x20; 
static const unsigned char string_header_8 = 0x01;
static const unsigned char string_header_16 = 0x02;
static const unsigned char string_header_32 = 0x03;

static const unsigned char string_enc_native = 0x00; 
static const unsigned char string_enc_utf8 = 0x40;
static const unsigned char string_enc_latin1 = 0x80;
static const unsigned char string_enc_bytes = 0xC0;

static const unsigned char attribute_header_5 = 0xE0;
static const unsigned char attribute_header_8 = 0x1E;
static const unsigned char attribute_header_32 = 0x1F;

static const unsigned char complex_header_32 = 0x15;
static const unsigned char complex_header_64 = 0x16;

static const unsigned char nstype_header_32 = 0x19;
static const unsigned char nstype_header_64 = 0x1A;

static const std::set<SEXPTYPE> stypes = {REALSXP, INTSXP, LGLSXP, STRSXP, CHARSXP, NILSXP, VECSXP, CPLXSXP, RAWSXP};

inline void writeSizeToFile8(std::ofstream & myFile, uint64_t x) {uint64_t x_temp = static_cast<uint64_t>(x); myFile.write(reinterpret_cast<char*>(&x_temp),8);}
inline void writeSizeToFile4(std::ofstream & myFile, uint64_t x) {uint32_t x_temp = static_cast<uint32_t>(x); myFile.write(reinterpret_cast<char*>(&x_temp),4);}
uint64_t readSizeFromFile4(std::ifstream & myFile) {
  std::array<char,4> a = {0,0,0,0};
  myFile.read(a.data(),4);
  return *reinterpret_cast<uint32_t*>(a.data());
}
uint64_t readSizeFromFile8(std::ifstream & myFile) {
  std::array<char,8> a = {0,0,0,0,0,0,0,0};
  myFile.read(a.data(),8);
  return *reinterpret_cast<uint64_t*>(a.data());
}

// unaligned cast to <POD>
template<typename POD>
inline POD unaligned_cast(char* data, uint64_t offset) {
  POD y;
  std::memcpy(&y, data + offset, sizeof(y));
  return y;
}


// qs reserve header details
// reserve[0] unused
// reserve[1] unused
// reserve[2] (low byte) shuffle control: 0x01 = logical shuffle, 0x02 = integer shuffle, 0x04 = double shuffle
// reserve[2] (high byte) algorithm: 0x10 = lz4, 0x00 = zstd
// reserve[3] endian: 1 = big endian, 0 = little endian
struct QsMetadata {
  unsigned char compress_algorithm;
  int compress_level;
  bool lgl_shuffle;
  bool int_shuffle;
  bool real_shuffle;
  bool cplx_shuffle;
  unsigned char endian;
  QsMetadata(std::string preset="balanced", std::string algorithm = "lz4", int compress_level=1, int shuffle_control=15) {
    if(preset == "fast") {
      this->compress_level = 150;
      shuffle_control = 0;
      compress_algorithm = 1;
    } else if(preset == "balanced") {
      this->compress_level = 1;
      shuffle_control = 15;
      compress_algorithm = 1;
    } else if(preset == "high") {
      this->compress_level = 4;
      shuffle_control = 15;
      compress_algorithm = 0;
    } else if(preset == "custom") {
      if(algorithm == "zstd") {
        compress_algorithm = 0;
        this->compress_level = compress_level;
        if(compress_level > 22 || compress_level < -50) throw exception("zstd compress_level must be an integer between -50 and 22");
      } else if(algorithm == "lz4") {
        compress_algorithm = 1;
        this->compress_level = compress_level;
        if(compress_level < 1) throw exception("lz4 compress_level must be an integer greater than 1");
      }  else if(algorithm == "lz4hc") {
        compress_algorithm = 2;
        this->compress_level = compress_level;
        if(compress_level < 1 || compress_level > 12) throw exception("lz4hc compress_level must be an integer between 1 and 12");
      } else {
        throw exception("algorithm must be one of zstd or lz4 or lz4hc");
      }
      if(shuffle_control < 0 || shuffle_control > 15) throw exception("shuffle_control must be an integer between 0 and 15");
    } else {
      throw exception("preset must be one of fast (default), balanced, high or custom");
    }
    lgl_shuffle = shuffle_control & 0x01;
    int_shuffle = shuffle_control & 0x02;
    real_shuffle = shuffle_control & 0x04;
    cplx_shuffle = shuffle_control & 0x08;
    endian = is_big_endian() ? 1 : 0;
  }
  QsMetadata(unsigned char shuffle_control, unsigned char algo, int cl) : 
    compress_algorithm(algo), compress_level(cl) {
    lgl_shuffle = shuffle_control & 0x01;
    int_shuffle = shuffle_control & 0x02;
    real_shuffle = shuffle_control & 0x04;
    cplx_shuffle = shuffle_control & 0x08;
    endian = is_big_endian() ? 1 : 0;
  }
  QsMetadata(std::ifstream & myFile) {
    std::array<unsigned char,4> reserve_bits;
    myFile.read(reinterpret_cast<char*>(reserve_bits.data()),4);
    unsigned char sys_endian = is_big_endian() ? 1 : 0;
    if(reserve_bits[3] != sys_endian) throw exception("Endian of system doesn't match file endian");
    compress_algorithm = reserve_bits[2] >> 4;
    compress_level = 1;
    lgl_shuffle = reserve_bits[2] & 0x01;
    int_shuffle = reserve_bits[2] & 0x02;
    real_shuffle = reserve_bits[2] & 0x04;
    cplx_shuffle = reserve_bits[2] & 0x08;
    endian = sys_endian;
  }
  void writeToFile(std::ofstream & myFile) {
    std::array<unsigned char,4> reserve_bits = {0,0,0,0};
    reserve_bits[2] += compress_algorithm << 4;
    reserve_bits[3] = is_big_endian() ? 1 : 0;
    reserve_bits[2] += (lgl_shuffle) + (int_shuffle << 1) + (real_shuffle << 2) + (cplx_shuffle << 3);
    myFile.write(reinterpret_cast<char*>(reserve_bits.data()),4); // some reserve bits for future use
  }
  void writeToFile(std::ofstream & myFile, unsigned char shuffle_control) {
    std::array<unsigned char,4> reserve_bits = {0,0,0,0};
    reserve_bits[2] += compress_algorithm << 4;
    reserve_bits[3] = is_big_endian() ? 1 : 0;
    reserve_bits[2] += shuffle_control;
    myFile.write(reinterpret_cast<char*>(reserve_bits.data()),4); // some reserve bits for future use
  }
};


// Normalize lz4/zstd function arguments so we can use function types
typedef size_t (*compress_fun)(void*, size_t, const void*, size_t, int);
typedef size_t (*decompress_fun)(void*, size_t, const void*, size_t);
typedef size_t (*cbound_fun)(size_t);

size_t LZ4_compressBound_fun(size_t srcSize) {
  return LZ4_compressBound(srcSize);
}

size_t LZ4_compress_fun( void* dst, size_t dstCapacity,
                         const void* src, size_t srcSize,
                         int compressionLevel) {
  return LZ4_compress_fast(reinterpret_cast<char*>(const_cast<void*>(src)), 
                           reinterpret_cast<char*>(const_cast<void*>(dst)),
                           static_cast<int>(srcSize), static_cast<int>(dstCapacity), compressionLevel);
}

size_t LZ4_compress_HC_fun( void* dst, size_t dstCapacity,
                         const void* src, size_t srcSize,
                         int compressionLevel) {
  return LZ4_compress_HC(reinterpret_cast<char*>(const_cast<void*>(src)), 
                           reinterpret_cast<char*>(const_cast<void*>(dst)),
                           static_cast<int>(srcSize), static_cast<int>(dstCapacity), compressionLevel);
}

size_t LZ4_decompress_fun( void* dst, size_t dstCapacity,
                           const void* src, size_t compressedSize) {
  return LZ4_decompress_safe(reinterpret_cast<char*>(const_cast<void*>(src)), 
                             reinterpret_cast<char*>(const_cast<void*>(dst)),
                             static_cast<int>(compressedSize), static_cast<int>(dstCapacity));
  
}

#endif
