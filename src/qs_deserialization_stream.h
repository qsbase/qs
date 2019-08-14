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
 https://github.com/traversc/qs
 */

#include "qs_common.h"

#define RESERVE_SIZE 4

template <class stream_reader>
struct ZSTD_streamRead {
  stream_reader & myFile;
  QsMetadata qm;
  uint64_t bytes_read;
  uint64_t minblocksize;
  uint64_t maxblocksize;
  uint64_t decompressed_bytes_read;
  std::vector<char> outblock;
  std::vector<char> inblock;
  ZSTD_inBuffer zin;
  ZSTD_outBuffer zout;
  ZSTD_DStream* zds;
  uint64_t blocksize; // shared with Data_Context_Stream by reference -- block_size
  uint64_t blockoffset; // shared with Data_Context_Stream by reference -- data_offset
  xxhash_env xenv;
  std::array<char, 4> hash_reserve;
  bool eof;
  ZSTD_streamRead(stream_reader & mf, QsMetadata qm) : 
    myFile(mf), qm(qm), xenv(xxhash_env()) {
    size_t outblocksize = 4*ZSTD_DStreamOutSize();
    size_t inblocksize = ZSTD_DStreamInSize();
    decompressed_bytes_read = 0;
    outblock = std::vector<char>(outblocksize);
    inblock = std::vector<char>(inblocksize);
    minblocksize = ZSTD_DStreamOutSize();
    maxblocksize = 4*ZSTD_DStreamOutSize();
    zds = ZSTD_createDStream();
    ZSTD_initDStream(zds);
    zout.size = maxblocksize;
    zout.pos = 0;
    zout.dst = outblock.data();
    zin.size = 0;
    zin.pos = 0;
    zin.src = inblock.data();
    blocksize = 0;
    blockoffset = 0;
    
    // need to reserve some bytes for the hash check at the end
    // ref https://stackoverflow.com/questions/22984956/tellg-function-give-wrong-size-of-file
    // std::streampos current = myFile.tellg();
    // myFile.ignore(std::numeric_limits<std::streamsize>::max());
    // readable_bytes = myFile.gcount();
    // myFile.seekg(current);
    // // std::cout << readable_bytes << std::endl;
    // if(qm.check_hash) readable_bytes -= 4;
    // // std::cout << readable_bytes << std::endl;
    bytes_read = 0;
    hash_reserve = {0,0,0,0};
    if(qm.check_hash) {
      read_check(myFile, hash_reserve.data(), RESERVE_SIZE);
    }
    eof = false;
  }
  //file at end of reserve

  size_t read_reserve(char * dst, size_t length, bool exact=false) {
    if(!qm.check_hash) {
      return read_check(myFile, dst, length, exact);
    }
    if(exact) {
      if(length >= RESERVE_SIZE) {
        std::memcpy(dst, hash_reserve.data(), RESERVE_SIZE);
        read_check(myFile, dst + RESERVE_SIZE, length - RESERVE_SIZE, true);
        read_check(myFile, hash_reserve.data(), RESERVE_SIZE, true);
      } else { // RESERVE_SIZE > length
        std::memcpy(dst, hash_reserve.data(), length);
        // since some of the reserve buffer was consumed, shift the unconsumed bytes to beginning of array
        // then read from file to fill up reserve buffer
        std::memmove(hash_reserve.data(), hash_reserve.data() + length, RESERVE_SIZE - length);
        read_check(myFile, hash_reserve.data() +  RESERVE_SIZE - length, length, true);
      }
      
      // for(unsigned int i=0; i < 4; i++) std::cout << std::hex << (int)reinterpret_cast<unsigned char &>(hash_reserve.data()[i]) << " "; std::cout << std::endl;
      return length;
    } else { // !exact -- we can't assume that "length" bytes are left in myFile; there could even be zero bytes left
      if(length >= RESERVE_SIZE) {
        // use "dst" as a temporary buffer, since it's already allocated
        // it is not a good idea to allocate a temp buffer of size length, as length can be large
        std::memcpy(dst, hash_reserve.data(), RESERVE_SIZE);
        size_t n_read = read_check(myFile, dst + RESERVE_SIZE, length - RESERVE_SIZE, false);
        size_t n_bufferable = n_read + RESERVE_SIZE;
        if(n_bufferable < length) {
          std::memcpy(hash_reserve.data(), dst + n_bufferable - RESERVE_SIZE, RESERVE_SIZE);

          // for(unsigned int i=0; i < 4; i++) std::cout << std::hex << (int)reinterpret_cast<unsigned char &>(hash_reserve.data()[i]) << " "; std::cout << std::endl;
          return n_bufferable - RESERVE_SIZE;
        } else {
          std::array<char, RESERVE_SIZE> temp_buffer = {0,0,0,0};
          size_t temp_size = read_check(myFile, temp_buffer.data(), RESERVE_SIZE, false);
          std::memcpy(hash_reserve.data(), dst + n_bufferable - (RESERVE_SIZE - temp_size), RESERVE_SIZE - temp_size);
          std::memcpy(hash_reserve.data() + RESERVE_SIZE - temp_size, temp_buffer.data(), temp_size);
          
          // for(unsigned int i=0; i < 4; i++) std::cout << std::hex << (int)reinterpret_cast<unsigned char &>(hash_reserve.data()[i]) << " "; std::cout << std::endl;
          return n_bufferable - (RESERVE_SIZE - temp_size);
        }
      } else { // length < RESERVE_SIZE
        std::vector<char> temp_buffer(length, '\0');
        size_t return_value = read_check(myFile, temp_buffer.data(), length, false);
        // n_bufferable is at most RESERVE_SIZE*2 - 1 = 7
        std::memcpy(dst, hash_reserve.data(), return_value);
        std::memmove(hash_reserve.data(), hash_reserve.data() + return_value, RESERVE_SIZE - return_value);
        std::memcpy(hash_reserve.data() + (RESERVE_SIZE - return_value), temp_buffer.data(), return_value);
        
        // for(unsigned int i=0; i < 4; i++) std::cout << std::hex << (int)reinterpret_cast<unsigned char &>(hash_reserve.data()[i]) << " "; std::cout << std::endl;
        return return_value;
      }
    }
  }
  ~ZSTD_streamRead() {
    ZSTD_freeDStream(zds);
  }
  inline void ZSTD_decompressStream_count(ZSTD_DStream* zds, ZSTD_outBuffer * zout, ZSTD_inBuffer * zin) {
    uint64_t temp = zout->pos;
    size_t return_value = ZSTD_decompressStream(zds, zout, zin);
    if(ZSTD_isError(return_value)) throw std::runtime_error("zstd stream decompression error");
    decompressed_bytes_read += zout->pos - temp;
    xenv.update(reinterpret_cast<char*>(zout->dst)+temp, zout->pos - temp);
    // for(unsigned int i=0; i < zout->pos - temp; i++) std::cout << std::hex << (int)(reinterpret_cast<unsigned char *>(zout->dst)[i+temp]) << " "; std::cout << std::endl;
    // std::cout << std::dec << xenv.digest() << std::endl;
  }
  void getBlock() {
    // if((qm.clength != 0) & (decompressed_bytes_read >= qm.clength)) return;
    if(eof) return;
    char * ptr = outblock.data();
    if(blocksize > blockoffset) {
      std::memmove(ptr, ptr + blockoffset, blocksize - blockoffset); 
      zout.pos = blocksize - blockoffset;
    } else {
      zout.pos = 0;
    }
    while(zout.pos < minblocksize) {
      if(zin.pos < zin.size) {
        ZSTD_decompressStream_count(zds, &zout, &zin);
      } else if(! eof) {
        uint64_t bytes_read = read_reserve(inblock.data(), inblock.size(), false);
        if(bytes_read == 0) {
          eof = true;
          continue; // EOF
        }
        zin.pos = 0;
        zin.size = bytes_read;
        ZSTD_decompressStream_count(zds, &zout, &zin);
      } else {
        size_t current_pos = zout.pos;
        ZSTD_decompressStream_count(zds, &zout, &zin);
        if(zout.pos == current_pos) break; // no more data
      }
    }
    blocksize = zout.pos;
    blockoffset = 0;
  }
  void copyData(char* dst,uint64_t dst_size) {
    char * ptr = outblock.data();
    // dst should never overlap since blocksize > MINBLOCKSIZE
    if(dst_size > blocksize - blockoffset) {
      // std::cout << blocksize - blockoffset << " data cpy\n";
      std::memcpy(dst, ptr + blockoffset, blocksize - blockoffset);
      zout.pos = blocksize - blockoffset;
      zout.dst = dst;
      zout.size = dst_size;
      while(zout.pos < dst_size) {
        // std::cout << zout.pos << " " << dst_size << " zout position\n";
        if(zin.pos < zin.size) {
          ZSTD_decompressStream_count(zds, &zout, &zin);
          // std::cout << zin.pos << "/" << zin.size << " zin " << zout.pos << "/" << zout.size << " zout\n";
        } else if(! eof) {
          uint64_t bytes_read = read_reserve(inblock.data(), minblocksize);
          if(bytes_read == 0) {
            eof = true;
            continue; // EOF
          }
          zin.pos = 0;
          zin.size = bytes_read;
          ZSTD_decompressStream_count(zds, &zout, &zin);
          // std::cout << zin.pos << "/" << zin.size << " zin " << zout.pos << "/" << zout.size << " zout new inblock\n";
        } else {
          size_t current_pos = zout.pos;
          ZSTD_decompressStream_count(zds, &zout, &zin);
          // std::cout << zin.pos << "/" << zin.size << " zin " << zout.pos << "/" << zout.size << " zout flush\n";
          if(zout.pos == current_pos) {
            Rcpp::Rcerr << "End of file reached, but more data was expected (object may be incomplete)" << std::endl;
            break; // no more data, also we should throw an error as more data was expected
          }
        }
      }
      blockoffset = 0;
      blocksize = 0;
    } else {
      std::memcpy(dst, ptr + blockoffset, dst_size);
      blockoffset += dst_size;
    }
    zout.dst = outblock.data();
    zout.size = maxblocksize;
    if(blocksize - blockoffset < BLOCKRESERVE) {
      getBlock();
    }
  }
};

template <class stream_reader>
struct uncompressed_streamRead {
  stream_reader & con;
  QsMetadata qm;
  std::vector<char> outblock;
  uint64_t blocksize; // shared with Data_Context_Stream by reference -- block_size
  uint64_t blockoffset; // shared with Data_Context_Stream by reference -- data_offset
  uint64_t decompressed_bytes_read; // same as total bytes read since no compression
  xxhash_env xenv;
  std::array<char, 4> hash_reserve;
  uncompressed_streamRead(stream_reader & _con, QsMetadata qm) : 
    con(_con), qm(qm),
    outblock(std::vector<char>(BLOCKSIZE+BLOCKRESERVE)), blocksize(0), blockoffset(0),
    decompressed_bytes_read(0), xenv(xxhash_env()) {
      hash_reserve = {0,0,0,0};
      if(qm.check_hash) {
        read_check(con, hash_reserve.data(), RESERVE_SIZE);
      }
    }
  
  // fread with updating hash as necessary
  size_t read_update(char * dst, size_t length, bool exact=false) {
    if(!qm.check_hash) {
      uint64_t return_value = read_check(con, dst, length, exact);
      decompressed_bytes_read += return_value;
      xenv.update(dst, return_value);
      return return_value;
    }
    if(exact) {
      if(length >= RESERVE_SIZE) {
        std::memcpy(dst, hash_reserve.data(), RESERVE_SIZE);
        read_check(con, dst + RESERVE_SIZE, length - RESERVE_SIZE, true);
        read_check(con, hash_reserve.data(), RESERVE_SIZE, true);
      } else { // RESERVE_SIZE > length
        std::memcpy(dst, hash_reserve.data(), length);
        // since some of the reserve buffer was consumed, shift the unconsumed bytes to beginning of array
        // then read from file to fill up reserve buffer
        std::memmove(hash_reserve.data(), hash_reserve.data() + length, RESERVE_SIZE - length);
        read_check(con, hash_reserve.data() +  RESERVE_SIZE - length, length, true);
      }
      decompressed_bytes_read += length;
      xenv.update(dst, length);
      return length;
    } else { // !exact -- we can't assume that "length" bytes are left in myFile; there could even be zero bytes left
      if(length >= RESERVE_SIZE) {
        // use "dst" as a temporary buffer, since it's already allocated
        // it is not a good idea to allocate a temp buffer of size length, as length can be large
        std::memcpy(dst, hash_reserve.data(), RESERVE_SIZE);
        size_t n_read = read_check(con, dst + RESERVE_SIZE, length - RESERVE_SIZE, false);
        size_t n_bufferable = n_read + RESERVE_SIZE;
        if(n_bufferable < length) {
          std::memcpy(hash_reserve.data(), dst + n_bufferable - RESERVE_SIZE, RESERVE_SIZE);
          decompressed_bytes_read += n_bufferable - RESERVE_SIZE;
          xenv.update(dst, n_bufferable - RESERVE_SIZE);
          return n_bufferable - RESERVE_SIZE;
        } else {
          std::array<char, RESERVE_SIZE> temp_buffer = {0,0,0,0};
          size_t temp_size = read_check(con, temp_buffer.data(), RESERVE_SIZE, false);
          std::memcpy(hash_reserve.data(), dst + n_bufferable - (RESERVE_SIZE - temp_size), RESERVE_SIZE - temp_size);
          std::memcpy(hash_reserve.data() + RESERVE_SIZE - temp_size, temp_buffer.data(), temp_size);
          decompressed_bytes_read += n_bufferable - (RESERVE_SIZE - temp_size);
          xenv.update(dst, n_bufferable - (RESERVE_SIZE - temp_size));
          return n_bufferable - (RESERVE_SIZE - temp_size);
        }
      } else { // length < RESERVE_SIZE
        std::vector<char> temp_buffer(length, '\0');
        size_t return_value = read_check(con, temp_buffer.data(), length, false);
        // n_bufferable is at most RESERVE_SIZE*2 - 1 = 7
        std::memcpy(dst, hash_reserve.data(), return_value);
        std::memmove(hash_reserve.data(), hash_reserve.data() + return_value, RESERVE_SIZE - return_value);
        std::memcpy(hash_reserve.data() + (RESERVE_SIZE - return_value), temp_buffer.data(), return_value);
        decompressed_bytes_read += return_value;
        xenv.update(dst, return_value);
        return return_value;
      }
    }
  }
  
  void getBlock() {
    char * ptr = outblock.data();
    uint64_t block_offset;
    if(blocksize > blockoffset) {
      std::memmove(ptr, ptr + blockoffset, blocksize - blockoffset);
      block_offset = blocksize - blockoffset;
    } else {
      block_offset = 0;
    }
    uint64_t bytes_read = read_update(ptr + block_offset, BLOCKSIZE - block_offset, false);
    // std::cout << bytes_read << std::endl;
    blocksize = block_offset + bytes_read;
    blockoffset = 0;
  }
  void copyData(char* dst,uint64_t dst_size) {
    char * ptr = outblock.data();
    if(dst_size > blocksize - blockoffset) {
      std::memcpy(dst, ptr + blockoffset, blocksize - blockoffset);
      uint64_t block_offset = blocksize - blockoffset;
      read_update(dst + block_offset, dst_size - block_offset, true);
      blockoffset = 0;
      blocksize = 0;
    } else {
      std::memcpy(dst, ptr + blockoffset, dst_size);
      blockoffset += dst_size;
    }
    if(blocksize - blockoffset < BLOCKRESERVE) {
      getBlock();
    }
  }
};


template <class DestreamClass> 
struct Data_Context_Stream {
  DestreamClass & dsc;
  QsMetadata qm;
  bool use_alt_rep_bool;
  std::vector<uint8_t> shuffleblock;
  uint64_t & data_offset; // dsc.blockoffset
  uint64_t & block_size; // dsc.blocksize
  char * data_ptr;
  std::string temp_string;
  
  Data_Context_Stream(DestreamClass & d, QsMetadata q, bool use_alt_rep) : dsc(d), qm(q), use_alt_rep_bool(use_alt_rep),
    shuffleblock(std::vector<uint8_t>(256)), data_offset(d.blockoffset), block_size(d.blocksize), data_ptr(d.outblock.data()), 
    temp_string(std::string(256, '\0')) {}
  
  void getBlock() {
    dsc.getBlock();
  }
  void getBlockData(char* outp, uint64_t data_size) {
    dsc.copyData(outp, data_size);
  }
  void readHeader(SEXPTYPE & object_type, uint64_t & r_array_len) {
    if(data_offset + BLOCKRESERVE >= block_size) getBlock();
    readHeader_common(object_type, r_array_len, data_offset, data_ptr);
  }
  void readStringHeader(uint32_t & r_string_len, cetype_t & ce_enc) {
    if(data_offset + BLOCKRESERVE >= block_size) getBlock();
    readStringHeader_common(r_string_len, ce_enc, data_offset, data_ptr);
  }
  void getShuffleBlockData(char* outp, uint64_t data_size, uint64_t bytesoftype) {
    // std::cout << data_size << " get shuffle block\n";
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
    // std::cout << r_array_len << " " << obj_type << "\n";
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
      obj = PROTECT(Rf_allocVector(VECSXP, r_array_len)); pt++;
      for(uint64_t i=0; i<r_array_len; i++) {
        SET_VECTOR_ELT(obj, i, processBlock());
      }
      break;
    case REALSXP:
      obj = PROTECT(Rf_allocVector(REALSXP, r_array_len)); pt++;
      if(qm.real_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(REAL(obj)), r_array_len*8, 8);
      } else {
        getBlockData(reinterpret_cast<char*>(REAL(obj)), r_array_len*8);
      }
      break;
    case INTSXP:
      obj = PROTECT(Rf_allocVector(INTSXP, r_array_len)); pt++;
      if(qm.int_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(INTEGER(obj)), r_array_len*4, 4);
      } else {
        getBlockData(reinterpret_cast<char*>(INTEGER(obj)), r_array_len*4);
      }
      break;
    case LGLSXP:
      obj = PROTECT(Rf_allocVector(LGLSXP, r_array_len)); pt++;
      if(qm.lgl_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(LOGICAL(obj)), r_array_len*4, 4);
      } else {
        getBlockData(reinterpret_cast<char*>(LOGICAL(obj)), r_array_len*4);
      }
      break;
    case CPLXSXP:
      obj = PROTECT(Rf_allocVector(CPLXSXP, r_array_len)); pt++;
      if(qm.cplx_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(COMPLEX(obj)), r_array_len*16, 8);
      } else {
        getBlockData(reinterpret_cast<char*>(COMPLEX(obj)), r_array_len*16);
      }
      break;
    case RAWSXP:
      obj = PROTECT(Rf_allocVector(RAWSXP, r_array_len)); pt++;
      if(r_array_len > 0) getBlockData(reinterpret_cast<char*>(RAW(obj)), r_array_len);
      break;
    case STRSXP:
#ifdef ALTREP_SUPPORTED
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
        obj = PROTECT(stdvec_string::Make(ret, true)); pt++;
      } else {
#endif
        obj = PROTECT(Rf_allocVector(STRSXP, r_array_len)); pt++;
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
#ifdef ALTREP_SUPPORTED
      }
#endif
      break;
    case S4SXP:
    {
      SEXP obj_data = PROTECT(Rf_allocVector(RAWSXP, r_array_len)); pt++;
      getBlockData(reinterpret_cast<char*>(RAW(obj_data)), r_array_len);
      obj = PROTECT(unserializeFromRaw(obj_data)); pt++;
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

#undef RESERVE_SIZE