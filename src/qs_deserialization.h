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

#include "qs_common.h"

////////////////////////////////////////////////////////////////
// de-serialization functions
////////////////////////////////////////////////////////////////

// using an explicit decompress context seems to be marginally slower (zstd v. 1.4.0)
struct zstd_decompress_env {
  // ZSTD_DCtx* zcs;
  // zstd_decompress_env() : zcs(ZSTD_createDCtx()) {}
  // ~zstd_decompress_env() {
  //   ZSTD_freeDCtx(zcs);
  // }
  size_t decompress( void* dst, size_t dstCapacity,
                     const void* src, size_t compressedSize) {
    // return ZSTD_decompress(dst, dstCapacity, src, compressedSize);
    // return ZSTD_decompressDCtx(zcs, dst, dstCapacity, src, compressedSize);
    size_t return_value = ZSTD_decompress(dst, dstCapacity, src, compressedSize);
    if(return_value == ZSTD_CONTENTSIZE_ERROR || return_value == ZSTD_CONTENTSIZE_UNKNOWN) throw exception("zstd decompression error");
    return return_value;
  }
  size_t compressBound(size_t srcSize) {
    return ZSTD_compressBound(srcSize);
  }
};

struct lz4_decompress_env {
  size_t decompress( void* dst, size_t dstCapacity,
                     const void* src, size_t compressedSize) {
    // int return_value = LZ4_decompress_safe(reinterpret_cast<char*>(const_cast<void*>(src)),
    //                                        reinterpret_cast<char*>(const_cast<void*>(dst)),
    //                                        static_cast<int>(compressedSize), static_cast<int>(dstCapacity));
    // if(return_value < 0) throw exception("lz4 decompression error");
    // return return_value;
    return LZ4_decompress_safe(reinterpret_cast<char*>(const_cast<void*>(src)),
                                           reinterpret_cast<char*>(const_cast<void*>(dst)),
                                           static_cast<int>(compressedSize), static_cast<int>(dstCapacity));
  }
  size_t compressBound(size_t srcSize) {
    return LZ4_compressBound(srcSize);
  }
};

template <class decompress_env> 
struct Data_Context {
  std::ifstream & myFile;
  bool use_alt_rep_bool;
  decompress_env denv;
  QsMetadata qm;
  
  uint64_t number_of_blocks;
  std::vector<char> zblock;
  std::vector<char> block;
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  uint64_t data_offset;
  uint64_t block_i;
  uint64_t block_size;
  std::string temp_string;
  
  Data_Context(std::ifstream & mf, QsMetadata qm, bool use_alt_rep) : 
    myFile(mf), use_alt_rep_bool(use_alt_rep), denv(decompress_env()), qm(qm) {
    number_of_blocks = readSizeFromFile8(myFile);
    zblock = std::vector<char>(denv.compressBound(BLOCKSIZE));
    block = std::vector<char>(BLOCKSIZE);
    data_offset = 0;
    block_i = 0;
    block_size = 0;
    temp_string = std::string(256, '\0');
  }
  void readHeader(SEXPTYPE & object_type, uint64_t & r_array_len) {
    if(data_offset >= block_size) decompress_block();
    char* header = block.data();
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
    throw exception("something went wrong (reading object header)");
  }
  void readStringHeader(uint32_t & r_string_len, cetype_t & ce_enc) {
    if(data_offset >= block_size) decompress_block();
    char* header = block.data();
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
    throw exception("something went wrong (reading string header)");
  }
  void decompress_direct(char* bpointer) {
    block_i++;
    std::array<char, 4> zsize_ar = {0,0,0,0};
    myFile.read(zsize_ar.data(), 4);
    uint64_t zsize = *reinterpret_cast<uint32_t*>(zsize_ar.data());
    myFile.read(zblock.data(), zsize);
    block_size = denv.decompress(bpointer, BLOCKSIZE, zblock.data(), zsize);
  }
  void decompress_block() {
    block_i++;
    std::array<char, 4> zsize_ar = {0,0,0,0};
    myFile.read(zsize_ar.data(), 4);
    uint64_t zsize = *reinterpret_cast<uint32_t*>(zsize_ar.data());
    myFile.read(zblock.data(), zsize);
    block_size = denv.decompress(block.data(), BLOCKSIZE, zblock.data(), zsize);
    data_offset = 0;
  }
  void getBlockData(char* outp, uint64_t data_size) {
    if(data_size <= block_size - data_offset) {
      memcpy(outp, block.data()+data_offset, data_size);
      data_offset += data_size;
    } else {
      uint64_t bytes_accounted = block_size - data_offset;
      memcpy(outp, block.data()+data_offset, bytes_accounted);
      while(bytes_accounted < data_size) {
        if(data_size - bytes_accounted >= BLOCKSIZE) {
          decompress_direct(outp+bytes_accounted);
          bytes_accounted += BLOCKSIZE;
        } else {
          decompress_block();
          std::memcpy(outp + bytes_accounted, block.data(), data_size - bytes_accounted);
          data_offset = data_size - bytes_accounted;
          bytes_accounted += data_offset;
        }
      }
    }
  }
  void getShuffleBlockData(char* outp, uint64_t data_size, uint64_t bytesoftype) {
    if(data_size >= MIN_SHUFFLE_ELEMENTS) {
      if(data_size > shuffleblock.size()) shuffleblock.resize(data_size);
      getBlockData(reinterpret_cast<char*>(shuffleblock.data()), data_size);
      blosc_unshuffle(shuffleblock.data(), reinterpret_cast<uint8_t*>(outp), data_size, bytesoftype);
    } else if(data_size > 0) {
      getBlockData(outp, data_size);
    }
  }
  SEXP processBlock() {
    SEXPTYPE obj_type;
    uint64_t r_array_len;
    readHeader(obj_type, r_array_len);
    uint64_t number_of_attributes;
    if(obj_type == ANYSXP) {
      number_of_attributes = r_array_len;
      readHeader(obj_type, r_array_len);
    } else {
      number_of_attributes = 0;
    }
    SEXP obj;
    Protect_Tracker pt = Protect_Tracker();
    switch(obj_type) {
    case VECSXP: 
      obj = PROTECT(Rf_allocVector(VECSXP, r_array_len));  pt++;
      for(uint64_t i=0; i<r_array_len; i++) {
        SET_VECTOR_ELT(obj, i, processBlock());
      }
      break;
    case REALSXP:
      obj = PROTECT(Rf_allocVector(REALSXP, r_array_len));  pt++;
      if(qm.real_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(REAL(obj)), r_array_len*8, 8);
      } else {
        getBlockData(reinterpret_cast<char*>(REAL(obj)), r_array_len*8);
      }
      break;
    case INTSXP:
      obj = PROTECT(Rf_allocVector(INTSXP, r_array_len));  pt++;
      if(qm.int_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(INTEGER(obj)), r_array_len*4, 4);
      } else {
        getBlockData(reinterpret_cast<char*>(INTEGER(obj)), r_array_len*4);
      }
      break;
    case LGLSXP:
      obj = PROTECT(Rf_allocVector(LGLSXP, r_array_len));  pt++;
      if(qm.lgl_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(LOGICAL(obj)), r_array_len*4, 4);
      } else {
        getBlockData(reinterpret_cast<char*>(LOGICAL(obj)), r_array_len*4);
      }
      break;
    case CPLXSXP:
      obj = PROTECT(Rf_allocVector(CPLXSXP, r_array_len));  pt++;
      if(qm.cplx_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(COMPLEX(obj)), r_array_len*16, 8);
      } else {
        getBlockData(reinterpret_cast<char*>(COMPLEX(obj)), r_array_len*16);
      }
      break;
    case RAWSXP:
      obj = PROTECT(Rf_allocVector(RAWSXP, r_array_len));  pt++;
      if(r_array_len > 0) getBlockData(reinterpret_cast<char*>(RAW(obj)), r_array_len);
      break;
    case STRSXP:
      if(use_alt_rep_bool) {
        auto ret = new stdvec_data(r_array_len);
        for(uint64_t i=0; i < r_array_len; i++) {
          uint32_t r_string_len;
          cetype_t string_encoding = CE_NATIVE;
          readStringHeader(r_string_len, string_encoding);
          if(r_string_len == NA_STRING_LENGTH) {
            ret->encodings[i] = 5;
          } else if(r_string_len == 0) {
            ret->encodings[i] = 1;
            ret->strings[i] = "";
          } else {
            switch(string_encoding) {
            case CE_NATIVE:
              ret->encodings[i] = 1;
              break;
            case CE_UTF8:
              ret->encodings[i] = 2;
              break;
            case CE_LATIN1:
              ret->encodings[i] = 3;
              break;
            case CE_BYTES:
              ret->encodings[i] = 4;
              break;
            default:
              ret->encodings[i] = 5;
            break;
            }
            ret->strings[i].resize(r_string_len);
            getBlockData(&(ret->strings[i])[0], r_string_len);
          }
        }
        obj = PROTECT(stdvec_string::Make(ret, true));  pt++;
      } else {
        obj = PROTECT(Rf_allocVector(STRSXP, r_array_len));  pt++;
        for(uint64_t i=0; i<r_array_len; i++) {
          uint32_t r_string_len;
          cetype_t string_encoding = CE_NATIVE;
          readStringHeader(r_string_len, string_encoding);
          if(r_string_len == NA_STRING_LENGTH) {
            SET_STRING_ELT(obj, i, NA_STRING);
          } else if(r_string_len == 0) {
            SET_STRING_ELT(obj, i, Rf_mkCharLen("", 0));
          } else if(r_string_len > 0) {
            if(r_string_len > temp_string.size()) {
              temp_string.resize(r_string_len);
            }
            getBlockData(&temp_string[0], r_string_len);
            SET_STRING_ELT(obj, i, Rf_mkCharLenCE(temp_string.data(), r_string_len, string_encoding));
          }
        }
      }
      break;
    case S4SXP:
    {
      SEXP obj_data = PROTECT(Rf_allocVector(RAWSXP, r_array_len));  pt++;
      getBlockData(reinterpret_cast<char*>(RAW(obj_data)), r_array_len);
      obj = PROTECT(unserializeFromRaw(obj_data));  pt++;
      // UNPROTECT(2);
      return obj;
    }
    default: // also NILSXP
      obj = R_NilValue;
      return obj;
    }
    if(number_of_attributes > 0) {
      for(uint64_t i=0; i<number_of_attributes; i++) {
        uint32_t r_string_len;
        cetype_t string_encoding;
        readStringHeader(r_string_len, string_encoding);
        if(r_string_len > temp_string.size()) {
          temp_string.resize(r_string_len);
        }
        std::string temp_attribute_string = std::string(r_string_len, '\0');
        getBlockData(&temp_attribute_string[0], r_string_len);
        Rf_setAttrib(obj, Rf_install(temp_attribute_string.data()), processBlock());
      }
    }
    // UNPROTECT(1);
    return std::move(obj);
  }
};
