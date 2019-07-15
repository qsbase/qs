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
#include <cstdio>
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
#include <stdint.h>

#include <atomic>
#include <thread>

#include <R.h>
#include <Rinternals.h>
#include <Rversion.h>

#include "RApiSerializeAPI.h"
#include "zstd.h"
#include "lz4.h"
#include "lz4hc.h"
#include "BLOSC/shuffle_routines.h"
#include "BLOSC/unshuffle_routines.h"

#include "xxhash/xxhash.c" // static linking

#include <R_ext/Rdynload.h>

using namespace Rcpp;





////////////////////////////////////////////////////////////////
// alt rep string class
////////////////////////////////////////////////////////////////
#if R_VERSION >= R_Version(3, 5, 0)
#define ALTREP_SUPPORTED

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
    SEXP data2 = Materialize(vec);
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
  
  static void string_Set_elt(SEXP vec, R_xlen_t i, SEXP new_val) {
    SEXP data2 = Materialize(vec);
    SET_STRING_ELT(data2, i, new_val);
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
    R_set_altstring_Set_elt_method(class_t, string_Set_elt);
  }
  
};

// static initialization of stdvec_double::class_t
R_altrep_class_t stdvec_string::class_t;

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_stdvec_double(DllInfo* dll){
  stdvec_string::Init(dll);
}

#else
// [[Rcpp::init]]
void init_stdvec_double(DllInfo* dll){
  // do nothing -- need a dummy function becasue of Rcpp attributes
}
#endif
////////////////////////////////////////////////////////////////
// common utility functions and constants
////////////////////////////////////////////////////////////////

bool is_big_endian();

#define BLOCKRESERVE 64
#define NA_STRING_LENGTH 4294967295 // 2^32-1 -- length used to signify NA value
#define MIN_SHUFFLE_ELEMENTS 4
#define BLOCKSIZE 524288

// static const uint64_t SHUFFLE_DONE_VAL = std::numeric_limits<uint64_t>::max();
// static const uint64_t BLOCKSIZE = 524288;

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
uint32_t readSizeFromFile4(std::ifstream & myFile) {
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

// file descriptor read and write with error checking
size_t fread_check(void * ptr, size_t count, FILE * stream, bool check_size=true) {
  size_t return_value = fread(ptr, 1, count, stream);
  if(ferror(stream)) throw std::runtime_error("error reading from stream");
  if(check_size) {
    if(return_value != count) {
      throw std::runtime_error("error reading from stream");
    }
  }
  return return_value;
}

// file descriptor read and write with error checking
size_t fwrite_check(void * ptr, size_t count, FILE * stream, bool check_size=true) {
  size_t return_value = fwrite(ptr, 1, count, stream);
  if(ferror(stream)) throw std::runtime_error("error writing to stream");
  if(check_size) {
    if(return_value != count) {
      throw std::runtime_error("error writing to stream");
    }
  }
  return return_value;
}

// maximum value is 7, reserve bit shared with shuffle bit
// if we need more slots we will have to use other reserve bits
enum class compalg : unsigned char {
  zstd = 0, lz4 = 1, lz4hc = 2, zstd_stream = 3, pipe = 4
};
// qs reserve header details
// reserve[0] unused
// reserve[1] (low byte) 1 = hash of serialized object written to last 4 bytes of file -- before 16.3, no hash check was performed
// reserve[1] (high byte) unused
// reserve[2] (low byte) shuffle control: 0x01 = logical shuffle, 0x02 = integer shuffle, 0x04 = double shuffle
// reserve[2] (high byte) algorithm: 0x01 = lz4, 0x00 = zstd, 0x02 = "lz4hc", 0x03 = zstd_stream
// reserve[3] endian: 1 = big endian, 0 = little endian
struct QsMetadata {
  unsigned char compress_algorithm;
  int compress_level;
  bool lgl_shuffle;
  bool int_shuffle;
  bool real_shuffle;
  bool cplx_shuffle;
  unsigned char endian;
  bool check_hash;
  
  //constructor from qsave
  QsMetadata(std::string preset, std::string algorithm, int compress_level, int shuffle_control, bool check_hash) {
    if(preset == "fast") {
      this->compress_level = 100;
      shuffle_control = 0;
      compress_algorithm = static_cast<unsigned char>(compalg::lz4);
    } else if(preset == "balanced") {
      this->compress_level = 1;
      shuffle_control = 15;
      compress_algorithm = static_cast<unsigned char>(compalg::lz4);
    } else if(preset == "high") {
      compress_algorithm = static_cast<unsigned char>(compalg::zstd);
      this->compress_level = 4;
      shuffle_control = 15;
    } else if(preset == "archive") {
      compress_algorithm = static_cast<unsigned char>(compalg::zstd_stream);
      this->compress_level = 14;
      shuffle_control = 15;
      compress_algorithm = static_cast<unsigned char>(compalg::zstd_stream);
    } else if(preset == "custom") {
      if(algorithm == "zstd") {
        compress_algorithm = static_cast<unsigned char>(compalg::zstd);
        this->compress_level = compress_level;
        if(compress_level > 22 || compress_level < -50) throw std::runtime_error("zstd compress_level must be an integer between -50 and 22");
      } else if(algorithm == "zstd_stream") {
        compress_algorithm = static_cast<unsigned char>(compalg::zstd_stream);
        this->compress_level = compress_level;
        if(compress_level > 22 || compress_level < -50) throw std::runtime_error("zstd compress_level must be an integer between -50 and 22");
      } else if(algorithm == "lz4") {
        compress_algorithm = static_cast<unsigned char>(compalg::lz4);
        this->compress_level = compress_level;
        if(compress_level < 1) throw std::runtime_error("lz4 compress_level must be an integer greater than 1");
      } else if(algorithm == "lz4hc") {
        compress_algorithm = static_cast<unsigned char>(compalg::lz4hc);
        this->compress_level = compress_level;
        if(compress_level < 1 || compress_level > 12) throw std::runtime_error("lz4hc compress_level must be an integer between 1 and 12");
      } else if(algorithm == "pipe") {
        compress_algorithm = static_cast<unsigned char>(compalg::pipe);
        this->compress_level = 0;
      } else {
        throw std::runtime_error("algorithm must be one of zstd, lz4, lz4hc or zstd_stream");
      }
    } else {
      throw std::runtime_error("preset must be one of fast, balanced (default), high, archive or custom");
    }
    if(shuffle_control < 0 || shuffle_control > 15) throw std::runtime_error("shuffle_control must be an integer between 0 and 15");
    lgl_shuffle = shuffle_control & 0x01;
    int_shuffle = shuffle_control & 0x02;
    real_shuffle = shuffle_control & 0x04;
    cplx_shuffle = shuffle_control & 0x08;
    endian = is_big_endian() ? 0x01 : 0x00;
    this->check_hash = check_hash;
  }
  
  // 0x0B0E0A0C
  bool checkMagicNumber(std::array<unsigned char, 4> & reserve_bits) {
    if(reserve_bits[0] != 0x0B) return false;
    if(reserve_bits[1] != 0x0E) return false;
    if(reserve_bits[2] != 0x0A) return false;
    if(reserve_bits[3] != 0x0C) return false;
    return true;
  }
  
  // constructor from q_read
  QsMetadata(std::ifstream & myFile) {
    std::array<unsigned char,4> reserve_bits = {0,0,0,0};
    myFile.read(reinterpret_cast<char*>(reserve_bits.data()),4);
    // version 2
    if(reserve_bits[0] != 0) {
      if(!checkMagicNumber(reserve_bits)) throw std::runtime_error("QS format not detected");
      myFile.ignore(4); // empty reserve bits
      myFile.read(reinterpret_cast<char*>(reserve_bits.data()),4);
    }
    unsigned char sys_endian = is_big_endian() ? 0x01 : 0x00;
    if(reserve_bits[3] != sys_endian) throw std::runtime_error("Endian of system doesn't match file endian");
    compress_algorithm = reserve_bits[2] >> 4;
    compress_level = 1;
    lgl_shuffle = reserve_bits[2] & 0x01;
    int_shuffle = reserve_bits[2] & 0x02;
    real_shuffle = reserve_bits[2] & 0x04;
    cplx_shuffle = reserve_bits[2] & 0x08;
    check_hash = reserve_bits[1];
    endian = reserve_bits[3];
  }
  
  // constructor from q_read_pipe
  // must have magic number bits; version 1 is not applicable for pipes
  QsMetadata(FILE * myPipe) {
    std::array<unsigned char,4> reserve_bits = {0,0,0,0};
    fread_check(reinterpret_cast<char*>(reserve_bits.data()), 4, myPipe);
    if(!checkMagicNumber(reserve_bits)) throw std::runtime_error("QS format not detected");
    fread_check(reinterpret_cast<char*>(reserve_bits.data()), 4, myPipe); // empty reserve bits
    fread_check(reinterpret_cast<char*>(reserve_bits.data()), 4, myPipe);
    unsigned char sys_endian = is_big_endian() ? 0x01 : 0x00;
    if(reserve_bits[3] != sys_endian) throw std::runtime_error("Endian of system doesn't match file endian");
    compress_algorithm = reserve_bits[2] >> 4;
    compress_level = 1;
    lgl_shuffle = reserve_bits[2] & 0x01;
    int_shuffle = reserve_bits[2] & 0x02;
    real_shuffle = reserve_bits[2] & 0x04;
    cplx_shuffle = reserve_bits[2] & 0x08;
    check_hash = reserve_bits[1];
    endian = reserve_bits[3];
  }
  
  // version 2
  void writeToFile(std::ofstream & myFile) {
    std::array<unsigned char,4> reserve_bits = {0x0B,0x0E,0x0A,0x0C};
    myFile.write(reinterpret_cast<char*>(reserve_bits.data()),4);
    reserve_bits = {0,0,0,0};
    myFile.write(reinterpret_cast<char*>(reserve_bits.data()),4);
    reserve_bits[1] = check_hash;
    reserve_bits[2] += compress_algorithm << 4;
    reserve_bits[3] = is_big_endian() ? 0x01 : 0x00;
    reserve_bits[2] += (lgl_shuffle) + (int_shuffle << 1) + (real_shuffle << 2) + (cplx_shuffle << 3);
    myFile.write(reinterpret_cast<char*>(reserve_bits.data()),4);
  }
  
  void writeToPipe(FILE * myPipe) {
    std::array<unsigned char,4> reserve_bits = {0x0B,0x0E,0x0A,0x0C};
    fwrite_check(reinterpret_cast<char*>(reserve_bits.data()),4, myPipe);
    reserve_bits = {0,0,0,0};
    fwrite_check(reinterpret_cast<char*>(reserve_bits.data()),4, myPipe);
    reserve_bits[1] = check_hash;
    reserve_bits[2] += compress_algorithm << 4;
    reserve_bits[3] = is_big_endian() ? 0x01 : 0x00;
    reserve_bits[2] += (lgl_shuffle) + (int_shuffle << 1) + (real_shuffle << 2) + (cplx_shuffle << 3);
    fwrite_check(reinterpret_cast<char*>(reserve_bits.data()),4, myPipe);
  }
};

// Normalize lz4/zstd function arguments so we can use function types
typedef size_t (*compress_fun)(void*, size_t, const void*, size_t, int);
typedef size_t (*decompress_fun)(void*, size_t, const void*, size_t);
typedef size_t (*cbound_fun)(size_t);
typedef unsigned (*iserror_fun)(size_t);

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
  int ret = LZ4_decompress_safe(reinterpret_cast<char*>(const_cast<void*>(src)), 
                             reinterpret_cast<char*>(const_cast<void*>(dst)),
                             static_cast<int>(compressedSize), static_cast<int>(dstCapacity));
  if(ret < 0) {
    return SIZE_MAX;
  } else {
    return ret;
  }
}
  
unsigned LZ4_isError_fun(size_t retval) {
  if(retval == SIZE_MAX) {
    return true;
  } else {
    return false;
  }
}


////////////////////////////////////////////////////////////////
// common utility functions for serialization (stream)
////////////////////////////////////////////////////////////////

template <class T>
void writeHeader_stream(SEXPTYPE object_type, uint64_t length, T * sobj) {
  switch(object_type) {
  case REALSXP:
    if(length < 32) {
      sobj->push_pod(static_cast<unsigned char>( numeric_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_8)), 1);
      sobj->push_pod(static_cast<uint8_t>(length) );
    } else if(length < 65536) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_16)), 1);
      sobj->push_pod(static_cast<uint16_t>(length) );
    } else if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length) );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length) );
    }
    return;
  case VECSXP:
    if(length < 32) {
      sobj->push_pod(static_cast<unsigned char>( list_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_8)), 1);
      sobj->push_pod(static_cast<uint8_t>(length) );
    } else if(length < 65536) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_16)), 1);
      sobj->push_pod(static_cast<uint16_t>(length) );
    } else if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length) );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length) );
    }
    return;
  case INTSXP:
    if(length < 32) {
      sobj->push_pod(static_cast<unsigned char>( integer_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_8)), 1);
      sobj->push_pod(static_cast<uint8_t>(length) );
    } else if(length < 65536) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_16)), 1);
      sobj->push_pod(static_cast<uint16_t>(length) );
    } else if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length) );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length) );
    }
    return;
  case LGLSXP:
    if(length < 32) {
      sobj->push_pod(static_cast<unsigned char>( logical_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_8)), 1);
      sobj->push_pod(static_cast<uint8_t>(length) );
    } else if(length < 65536) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_16)), 1);
      sobj->push_pod(static_cast<uint16_t>(length) );
    } else if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length) );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length) );
    }
    return;
  case RAWSXP:
    if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&raw_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length) );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&raw_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length) );
    }
    return;
  case STRSXP:
    if(length < 32) {
      sobj->push_pod(static_cast<unsigned char>( character_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_8)), 1);
      sobj->push_pod(static_cast<uint8_t>(length) );
    } else if(length < 65536) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_16)), 1);
      sobj->push_pod(static_cast<uint16_t>(length) );
    } else if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length) );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length) );
    }
    return;
  case CPLXSXP:
    if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&complex_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length) );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&complex_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length) );
    }
    return;
  case NILSXP:
    sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&null_header)), 1);
    return;
  default:
    throw std::runtime_error("something went wrong writing object header");  // should never reach here
  }
}

template <class T>
void writeAttributeHeader_stream(uint64_t length, T * sobj) {
  if(length < 32) {
    sobj->push_pod(static_cast<unsigned char>( attribute_header_5 | static_cast<unsigned char>(length) ) );
  } else if(length < 256) {
    sobj->push_pod(static_cast<unsigned char>( attribute_header_8 ) );
    sobj->push_pod(static_cast<uint8_t>(length) );
  } else {
    sobj->push_pod(static_cast<unsigned char>( attribute_header_32 ) );
    sobj->push_pod(static_cast<uint32_t>(length) );
  }
}

template <class T>
void writeStringHeader_stream(uint64_t length, cetype_t ce_enc, T * sobj) {
  unsigned char enc;
  switch(ce_enc) {
  case CE_NATIVE:
    enc = string_enc_native; break;
  case CE_UTF8:
    enc = string_enc_utf8; break;
  case CE_LATIN1:
    enc = string_enc_latin1; break;
  case CE_BYTES:
    enc = string_enc_bytes; break;
  default:
    enc = string_enc_native;
  }
  if(length < 32) {
    sobj->push_pod(static_cast<unsigned char>( string_header_5 | static_cast<unsigned char>(enc) | static_cast<unsigned char>(length) ) );
  } else if(length < 256) {
    sobj->push_pod(static_cast<unsigned char>( string_header_8 | static_cast<unsigned char>(enc) ) );
    sobj->push_pod(static_cast<uint8_t>(length) );
  } else if(length < 65536) {
    sobj->push_pod(static_cast<unsigned char>( string_header_16 | static_cast<unsigned char>(enc) ) );
    sobj->push_pod(static_cast<uint16_t>(length) );
  } else {
    sobj->push_pod(static_cast<unsigned char>( string_header_32 | static_cast<unsigned char>(enc) ) );
    sobj->push_pod(static_cast<uint32_t>(length) );
  }
}

////////////////////////////////////////////////////////////////
// common utility functions for serialization (block compression)
////////////////////////////////////////////////////////////////

template <class T>
void writeHeader_common(SEXPTYPE object_type, uint64_t length, T * sobj) {
  switch(object_type) {
  case REALSXP:
    if(length < 32) {
      sobj->push_pod(static_cast<unsigned char>( numeric_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_8)), 1);
      sobj->push_pod(static_cast<uint8_t>(length), true );
    } else if(length < 65536) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_16)), 1);
      sobj->push_pod(static_cast<uint16_t>(length), true );
    } else if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length), true );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length), true );
    }
    return;
  case VECSXP:
    if(length < 32) {
      sobj->push_pod(static_cast<unsigned char>( list_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_8)), 1);
      sobj->push_pod(static_cast<uint8_t>(length), true );
    } else if(length < 65536) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_16)), 1);
      sobj->push_pod(static_cast<uint16_t>(length), true );
    } else if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length), true );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length), true );
    }
    return;
  case INTSXP:
    if(length < 32) {
      sobj->push_pod(static_cast<unsigned char>( integer_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_8)), 1);
      sobj->push_pod(static_cast<uint8_t>(length), true );
    } else if(length < 65536) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_16)), 1);
      sobj->push_pod(static_cast<uint16_t>(length), true );
    } else if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length), true );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length), true );
    }
    return;
  case LGLSXP:
    if(length < 32) {
      sobj->push_pod(static_cast<unsigned char>( logical_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_8)), 1);
      sobj->push_pod(static_cast<uint8_t>(length), true );
    } else if(length < 65536) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_16)), 1);
      sobj->push_pod(static_cast<uint16_t>(length), true );
    } else if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length), true );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length), true );
    }
    return;
  case RAWSXP:
    if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&raw_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length), true );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&raw_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length), true );
    }
    return;
  case STRSXP:
    if(length < 32) {
      sobj->push_pod(static_cast<unsigned char>( character_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_8)), 1);
      sobj->push_pod(static_cast<uint8_t>(length), true );
    } else if(length < 65536) { 
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_16)), 1);
      sobj->push_pod(static_cast<uint16_t>(length), true );
    } else if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length), true );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length), true );
    }
    return;
  case CPLXSXP:
    if(length < 4294967296) {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&complex_header_32)), 1);
      sobj->push_pod(static_cast<uint32_t>(length), true );
    } else {
      sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&complex_header_64)), 1);
      sobj->push_pod(static_cast<uint64_t>(length), true );
    }
    return;
  case NILSXP:
    sobj->push(reinterpret_cast<char*>(const_cast<unsigned char*>(&null_header)), 1);
    return;
  default:
    throw std::runtime_error("something went wrong writing object header");  // should never reach here
  }
}
template <class T>
void writeAttributeHeader_common(uint64_t length, T * sobj) {
  if(length < 32) {
    sobj->push_pod(static_cast<unsigned char>( attribute_header_5 | static_cast<unsigned char>(length) ) );
  } else if(length < 256) {
    sobj->push_pod(static_cast<unsigned char>( attribute_header_8 ) );
    sobj->push_pod(static_cast<uint8_t>(length), true );
  } else {
    sobj->push_pod(static_cast<unsigned char>( attribute_header_32 ) );
    sobj->push_pod(static_cast<uint32_t>(length), true );
  }
}

template <class T>
void writeStringHeader_common(uint64_t length, cetype_t ce_enc, T * sobj) {
  unsigned char enc;
  switch(ce_enc) {
  case CE_NATIVE:
    enc = string_enc_native; break;
  case CE_UTF8:
    enc = string_enc_utf8; break;
  case CE_LATIN1:
    enc = string_enc_latin1; break;
  case CE_BYTES:
    enc = string_enc_bytes; break;
  default:
    enc = string_enc_native;
  }
  if(length < 32) {
    sobj->push_pod(static_cast<unsigned char>( string_header_5 | static_cast<unsigned char>(enc) | static_cast<unsigned char>(length) ) );
  } else if(length < 256) {
    sobj->push_pod(static_cast<unsigned char>( string_header_8 | static_cast<unsigned char>(enc) ) );
    sobj->push_pod(static_cast<uint8_t>(length), true );
  } else if(length < 65536) {
    sobj->push_pod(static_cast<unsigned char>( string_header_16 | static_cast<unsigned char>(enc) ) );
    sobj->push_pod(static_cast<uint16_t>(length), true );
  } else {
    sobj->push_pod(static_cast<unsigned char>( string_header_32 | static_cast<unsigned char>(enc) ) );
    sobj->push_pod(static_cast<uint32_t>(length), true );
  }
}

////////////////////////////////////////////////////////////////
// common utility functions for deserialization
////////////////////////////////////////////////////////////////

uint32_t validate_hash(QsMetadata qm, std::ifstream & myFile, uint32_t computed_hash, bool strict) {
  if(qm.check_hash) {
    if(myFile.peek() == EOF) {
      if(strict) throw std::runtime_error("Warning: end of file reached, but hash checksum expected, data may be corrupted");
      Rcerr << "Warning: end of file reached, but hash checksum expected, data may be corrupted" << std::endl;
    } else {
      std::array<char,4> a = {0,0,0,0};
      myFile.read(a.data(),4);
      if(myFile.gcount() != 4) {
        if(strict) throw std::runtime_error("Warning: hash checksum expected, but not found, data may be corrupted");
        Rcerr << "Warning: hash checksum expected, but not found, data may be corrupted" << std::endl;
      }
      uint32_t recorded_hash = *reinterpret_cast<uint32_t*>(a.data());
      if(computed_hash != recorded_hash) {
        if(strict) throw std::runtime_error("Warning: hash checksum does not match ( " + 
           std::to_string(recorded_hash) + "," + std::to_string(computed_hash) + "), data may be corrupted");
        Rcerr << "Warning: hash checksum does not match ( " << recorded_hash << "," << computed_hash << "), data may be corrupted" << std::endl;
      }
      return recorded_hash;
      // std::cout << "hashes: computed, recorded: " << computed_hash << ", " << recorded_hash << std::endl;
    }
  } else {
    if(myFile.peek() != EOF) {
      if(strict) throw std::runtime_error("Warning: end of file expected but not reached, data may be corrupted");
      Rcerr << "Warning: end of file expected but not reached, data may be corrupted" << std::endl;
    }
  }
  return 0;
}

// current does not check end of stream -- should this be checked?
uint32_t validate_hash(QsMetadata qm, FILE * myPipe, uint32_t computed_hash, bool strict) {
  if(qm.check_hash) {
    std::array<char,4> a = {0,0,0,0};
    fread_check(a.data(),4, myPipe);
    uint32_t recorded_hash = *reinterpret_cast<uint32_t*>(a.data());
    if(computed_hash != recorded_hash) {
      if(strict) throw std::runtime_error("Warning: hash checksum does not match ( " + 
         std::to_string(recorded_hash) + "," + std::to_string(computed_hash) + "), data may be corrupted");
      Rcerr << "Warning: hash checksum does not match ( " << recorded_hash << "," << computed_hash << "), data may be corrupted" << std::endl;
    }
    return recorded_hash;
  } else {
    return 0;
  }
}

// R stack tracker using RAII
// protection handling using RAII should have no issues with longjmp
// ref: https://developer.r-project.org/Blog/public/2019/03/28/use-of-c---in-packages/
// "R restores the protection stack depth before taking a long jump,
// so if a C++ destructor includes say UNPROTECT(1) call to restore 
// the protection stack depth, it does not matter it is not executed, 
// because R will do that automatically."
// 
// There is also a limit of 10,000 on the protection stack.  
// Theoretically, we'd need to track it globally to be 100% error proof.  
// Realistically, it should never occur in this package as you'd need a list with depth 10,000.
// Ref: https://cran.r-project.org/doc/manuals/r-release/R-exts.html#Garbage-Collection
struct Protect_Tracker {
  unsigned int n;
  Protect_Tracker() : n(0) {}
  ~Protect_Tracker() {
    UNPROTECT(n);
  }
  void operator++(int) {
    n++;
  }
};

inline void readHeader_common(SEXPTYPE & object_type, uint64_t & r_array_len, uint64_t & data_offset, char* header) {
  unsigned char h5 = reinterpret_cast<unsigned char*>(header)[data_offset] & 0xE0;
  switch(h5) {
  case numeric_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = REALSXP;
    return;
  case list_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = VECSXP;
    return;
  case integer_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = INTSXP;
    return;
  case logical_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = LGLSXP;
    return;
  case character_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = STRSXP;
    return;
  case attribute_header_5:
    r_array_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = ANYSXP;
    return;
  }
  unsigned char hd = reinterpret_cast<unsigned char*>(header)[data_offset];
  switch(hd) {
  case numeric_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = REALSXP;
    return;
  case numeric_header_16:
    r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
    data_offset += 3;
    object_type = REALSXP;
    return;
  case numeric_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = REALSXP;
    return;
  case numeric_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = REALSXP;
    return;
  case list_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = VECSXP;
    return;
  case list_header_16:
    r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
    data_offset += 3;
    object_type = VECSXP;
    return;
  case list_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = VECSXP;
    return;
  case list_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = VECSXP;
    return;
  case integer_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = INTSXP;
    return;
  case integer_header_16:
    r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
    data_offset += 3;
    object_type = INTSXP;
    return;
  case integer_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = INTSXP;
    return;
  case integer_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = INTSXP;
    return;
  case logical_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = LGLSXP;
    return;
  case logical_header_16:
    r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
    data_offset += 3;
    object_type = LGLSXP;
    return;
  case logical_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = LGLSXP;
    return;
  case logical_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = LGLSXP;
    return;
  case raw_header_32:
    r_array_len = unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = RAWSXP;
    return;
  case raw_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = RAWSXP;
    return;
  case character_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = STRSXP;
    return;
  case character_header_16:
    r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
    data_offset += 3;
    object_type = STRSXP;
    return;
  case character_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = STRSXP;
    return;
  case character_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = STRSXP;
    return;
  case complex_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = CPLXSXP;
    return;
  case complex_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = CPLXSXP;
    return;
  case null_header:
    r_array_len =  0;
    data_offset += 1;
    object_type = NILSXP;
    return;
  case attribute_header_8:
    r_array_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = ANYSXP;
    return;
  case attribute_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = ANYSXP;
    return;
  case nstype_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = S4SXP;
    return;
  case nstype_header_64:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = S4SXP;
    return;
  }
  // additional types
  throw std::runtime_error("something went wrong (reading object header)");
}

inline void readStringHeader_common(uint32_t & r_string_len, cetype_t & ce_enc, uint64_t & data_offset, char* header) {
  unsigned char enc = reinterpret_cast<unsigned char*>(header)[data_offset] & 0xC0;
  switch(enc) {
  case string_enc_native:
    ce_enc = CE_NATIVE; break;
  case string_enc_utf8:
    ce_enc = CE_UTF8; break;
  case string_enc_latin1:
    ce_enc = CE_LATIN1; break;
  case string_enc_bytes:
    ce_enc = CE_BYTES; break;
  }
  
  if((reinterpret_cast<unsigned char*>(header)[data_offset] & 0x20) == string_header_5) {
    r_string_len = *reinterpret_cast<uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    return;
  } else {
    unsigned char hd = reinterpret_cast<unsigned char*>(header)[data_offset] & 0x1F;
    switch(hd) {
    case string_header_8:
      r_string_len =  *reinterpret_cast<uint8_t*>(header+data_offset+1) ;
      data_offset += 2;
      return;
    case string_header_16:
      r_string_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
      data_offset += 3;
      return;
    case string_header_32:
      r_string_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
      data_offset += 5;
      return;
    case string_header_NA:
      r_string_len = NA_STRING_LENGTH;
      data_offset += 1;
      return;
    }
  } 
  throw std::runtime_error("something went wrong (reading string header)");
}

////////////////////////////////////////////////////////////////
// Compress and decompress templates
////////////////////////////////////////////////////////////////

#define XXH_SEED 12345
// seed is 12345
struct xxhash_env {
  XXH32_state_s* x;
  xxhash_env() : x(XXH32_createState()) {
    XXH_errorcode ret = XXH32_reset(x, XXH_SEED);
    if(ret == XXH_ERROR) throw std::runtime_error("error in hashing function");
  }
  ~xxhash_env() {
    XXH32_freeState(x);
  }
  void reset() {
    XXH_errorcode ret = XXH32_reset(x, XXH_SEED);
    if(ret == XXH_ERROR) throw std::runtime_error("error in hashing function");
  }
  void update(const void* input, size_t length) {
    XXH_errorcode ret = XXH32_update(x, input, length);
    if(ret == XXH_ERROR) throw std::runtime_error("error in hashing function");
    // std::cout << digest() << std::endl;
  }
  uint32_t digest() {
    return XXH32_digest(x);
  }
};

struct zstd_compress_env {
  // ZSTD_CCtx* zcs;
  // zstd_compress_env() : zcs(ZSTD_createCCtx()) {}
  // ~zstd_compress_env() {
  //   ZSTD_freeCCtx(zcs);
  // }
  size_t compress( void* dst, size_t dstCapacity,
                   const void* src, size_t srcSize,
                   int compressionLevel) {
    // return ZSTD_compressCCtx(zcs, dst, dstCapacity, src, srcSize, compressionLevel);
    size_t return_value = ZSTD_compress(dst, dstCapacity, src, srcSize, compressionLevel);
    if(ZSTD_isError(return_value)) throw std::runtime_error("zstd compression error");
    return return_value;
  }
  size_t compressBound(size_t srcSize) {
    return ZSTD_compressBound(srcSize);
  }
};

struct lz4_compress_env {
  // std::vector<char> zcs;
  // char* state;
  // lz4_compress_env() {
  //   zcs = std::vector<char>(LZ4_sizeofState());
  //   state = zcs.data();
  // }
  size_t compress( void* dst, size_t dstCapacity,
                   const void* src, size_t srcSize,
                   int compressionLevel) {
    // return LZ4_compress_fast_extState(state, reinterpret_cast<char*>(const_cast<void*>(src)), 
    //                          reinterpret_cast<char*>(const_cast<void*>(dst)),
    //                          static_cast<int>(srcSize), static_cast<int>(dstCapacity), compressionLevel);
    int return_value = LZ4_compress_fast(reinterpret_cast<char*>(const_cast<void*>(src)), 
                                         reinterpret_cast<char*>(const_cast<void*>(dst)),
                                         static_cast<int>(srcSize), static_cast<int>(dstCapacity), 
                                         compressionLevel);
    if(return_value == 0) throw std::runtime_error("lz4 compression error");
    return return_value;
  }
  size_t compressBound(size_t srcSize) {
    return LZ4_compressBound(srcSize);
  }
};

struct lz4hc_compress_env {
  // std::vector<char> zcs;
  // char* state;
  // lz4hc_compress_env() {
  //   zcs = std::vector<char>(LZ4_sizeofStateHC());
  //   state = zcs.data();
  // }
  size_t compress( void* dst, size_t dstCapacity,
                   const void* src, size_t srcSize,
                   int compressionLevel) {
    // xenv.update(src, srcSize);
    // return LZ4_compress_HC_extStateHC(state, reinterpret_cast<char*>(const_cast<void*>(src)), 
    //                                   reinterpret_cast<char*>(const_cast<void*>(dst)),
    //                                   static_cast<int>(srcSize), static_cast<int>(dstCapacity), compressionLevel);
    int return_value = LZ4_compress_HC(reinterpret_cast<char*>(const_cast<void*>(src)), 
                                       reinterpret_cast<char*>(const_cast<void*>(dst)),
                                       static_cast<int>(srcSize), static_cast<int>(dstCapacity), 
                                       compressionLevel);
    if(return_value == 0) throw std::runtime_error("lz4hc compression error");
    return return_value;
  }
  size_t compressBound(size_t srcSize) {
    return LZ4_compressBound(srcSize);
  }
};


// Explicit decompression context (zstd v. 1.4.0)
struct zstd_decompress_env {
  // ZSTD_DCtx* zcs;
  // zstd_decompress_env() : zcs(ZSTD_createDCtx()) {}
  // ~zstd_decompress_env() {
  //   ZSTD_freeDCtx(zcs);
  // }
  uint64_t bound;
  zstd_decompress_env() : bound(ZSTD_compressBound(BLOCKSIZE)) {}
  size_t decompress( void* dst, size_t dstCapacity,
                     const void* src, size_t compressedSize) {
    // return ZSTD_decompress(dst, dstCapacity, src, compressedSize);
    // return ZSTD_decompressDCtx(zcs, dst, dstCapacity, src, compressedSize);
    if(compressedSize > bound) throw std::runtime_error("Malformed compress block: compressed size > compress bound");
    // std::cout << "decompressing " << dst << " " << dstCapacity << " " << src << " " << compressedSize << "\n";
    size_t return_value = ZSTD_decompress(dst, dstCapacity, src, compressedSize);
    if(ZSTD_isError(return_value)) throw std::runtime_error("zstd decompression error");
    if(return_value > BLOCKSIZE) throw std::runtime_error("Malformed compress block: decompressed size > max blocksize " + std::to_string(return_value));
    return return_value;
  }
  size_t compressBound(size_t srcSize) {
    return ZSTD_compressBound(srcSize);
  }
};

struct lz4_decompress_env {
  uint64_t bound;
  lz4_decompress_env() : bound(LZ4_compressBound(BLOCKSIZE)) {}
  size_t decompress( void* dst, size_t dstCapacity,
                     const void* src, size_t compressedSize) {
    // std::cout << "decomp " << compressedSize << std::endl;
    if(compressedSize > bound) throw std::runtime_error("Malformed compress block: compressed size > compress bound");
    int return_value = LZ4_decompress_safe(reinterpret_cast<char*>(const_cast<void*>(src)),
                                           reinterpret_cast<char*>(const_cast<void*>(dst)),
                                           static_cast<int>(compressedSize), static_cast<int>(dstCapacity));
    if(return_value < 0) throw std::runtime_error("lz4 decompression error");
    if(return_value > BLOCKSIZE) throw std::runtime_error("Malformed compress block: decompressed size > max blocksize" + std::to_string(return_value));
    return return_value;
    // return LZ4_decompress_safe(reinterpret_cast<char*>(const_cast<void*>(src)),
    //                                        reinterpret_cast<char*>(const_cast<void*>(dst)),
    //                                        static_cast<int>(compressedSize), static_cast<int>(dstCapacity));
  }
  size_t compressBound(size_t srcSize) {
    return LZ4_compressBound(srcSize);
  }
};


////////////////////////////////////////////////////////////////
// qdump/debug helper functions
////////////////////////////////////////////////////////////////

void dumpMetadata(List& output, QsMetadata& qm) {
  switch(qm.compress_algorithm) {
  case 0:
    output["compress_algorithm"] = "zstd";
    break;
  case 1:
    output["compress_algorithm"] = "lz4";
    break;
  case 2:
    output["compress_algorithm"] = "lz4hc";
    break;
  case 3:
    output["compress_algorithm"] = "zstd_stream";
    break;
  default:
    output["compress_algorithm"] = "unknown";
    break;
  }
  output["compress_level"] = qm.compress_level;
  output["lgl_shuffle"] = qm.lgl_shuffle;
  output["int_shuffle"] = qm.int_shuffle;
  output["real_shuffle"] = qm.real_shuffle;
  output["cplx_shuffle"] = qm.cplx_shuffle;
  output["endian"] = static_cast<int>(qm.endian);
  output["check_hash"] = qm.check_hash;
}

// simple decompress stream context

struct zstd_decompress_stream_simple {
  ZSTD_inBuffer zin;
  ZSTD_outBuffer zout;
  ZSTD_DStream* zds;
  
  zstd_decompress_stream_simple(char* outp, size_t outsize, char* inp, size_t insize) {
    zout.pos = 0;
    zout.dst = outp;
    zout.size = outsize;
    zin.pos = 0;
    zin.src = inp;
    zin.size = insize;
    zds = ZSTD_createDStream();
  }
  
  // returns true if error
  bool decompress() {
    while(zout.pos < zout.size) {
      size_t return_value = ZSTD_decompressStream(zds, &zout, &zin);
      if(ZSTD_isError(return_value)) return true;
    }
    return false;
  }
  ~zstd_decompress_stream_simple() {
    ZSTD_freeDStream(zds);
  }
};

#endif

