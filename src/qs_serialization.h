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

////////////////////////////////////////////////////////////////
// serialization functions
////////////////////////////////////////////////////////////////

template <class stream_writer, class compress_env> 
struct CompressBuffer {
  QsMetadata qm;
  stream_writer & myFile;
  compress_env cenv; // default constructor
  xxhash_env xenv; // default constructor
  uint64_t number_of_blocks = 0;
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  std::vector<char> block = std::vector<char>(BLOCKSIZE);
  uint64_t current_blocksize=0;
  std::vector<char> zblock = std::vector<char>(cenv.compressBound(BLOCKSIZE));
  CompressBuffer(stream_writer & f, QsMetadata qm) : qm(qm), myFile(f) {}
  void flush() {
    if(current_blocksize > 0) {
      uint64_t zsize = cenv.compress(zblock.data(), zblock.size(), block.data(), current_blocksize, qm.compress_level);
      writeSize4(myFile, zsize);
      write_check(myFile, zblock.data(), zsize);
      current_blocksize = 0;
      number_of_blocks++;
    }
  }
  void push_contiguous(const char * const data, const uint64_t len) {
    if(qm.check_hash) xenv.update(data, len);
    uint64_t current_pointer_consumed = 0;
    while(current_pointer_consumed < len) {
      if(current_blocksize == BLOCKSIZE) {
        flush();
      }
      if(current_blocksize == 0 && len - current_pointer_consumed >= BLOCKSIZE) {
        uint64_t zsize = cenv.compress(zblock.data(), zblock.size(), data + current_pointer_consumed, BLOCKSIZE, qm.compress_level);
        writeSize4(myFile, zsize);
        write_check(myFile, zblock.data(), zsize);
        current_pointer_consumed += BLOCKSIZE;
        number_of_blocks++;
      } else {
        uint64_t remaining_pointer_available = len - current_pointer_consumed;
        uint64_t add_length = remaining_pointer_available < (BLOCKSIZE - current_blocksize) ? remaining_pointer_available : BLOCKSIZE-current_blocksize;
        memcpy(block.data() + current_blocksize, data + current_pointer_consumed, add_length);
        current_blocksize += add_length;
        current_pointer_consumed += add_length;
      }
    }
  }
  void push_noncontiguous(const char * const data, const uint64_t len) {
    if(qm.check_hash) xenv.update(data, len);
    uint64_t current_pointer_consumed = 0;
    while(current_pointer_consumed < len) {
      if(BLOCKSIZE - current_blocksize < BLOCKRESERVE) {
        flush();
      }
      if(current_blocksize == 0 && len - current_pointer_consumed >= BLOCKSIZE) {
        uint64_t zsize = cenv.compress(zblock.data(), zblock.size(), data + current_pointer_consumed, BLOCKSIZE, qm.compress_level);
        writeSize4(myFile, zsize);
        write_check(myFile, zblock.data(), zsize);
        current_pointer_consumed += BLOCKSIZE;
        number_of_blocks++;
      } else {
        uint64_t remaining_pointer_available = len - current_pointer_consumed;
        uint64_t add_length = remaining_pointer_available < (BLOCKSIZE - current_blocksize) ? remaining_pointer_available : BLOCKSIZE-current_blocksize;
        memcpy(block.data() + current_blocksize, data + current_pointer_consumed, add_length);
        current_blocksize += add_length;
        current_pointer_consumed += add_length;
      }
    }
  }
  template<typename POD>
  inline void push_pod_contiguous(const POD pod) {
    push_contiguous(reinterpret_cast<const char * const>(&pod), sizeof(pod));
  }
  template<typename POD>
  inline void push_pod_noncontiguous(const POD pod) {
    push_noncontiguous(reinterpret_cast<const char * const>(&pod), sizeof(pod));
  }
  void shuffle_push(const char * const data, const uint64_t len, const uint64_t bytesoftype) {
    if(len > MIN_SHUFFLE_ELEMENTS) {
      if(len > shuffleblock.size()) shuffleblock.resize(len);
      blosc_shuffle(reinterpret_cast<const uint8_t * const>(data), shuffleblock.data(), len, bytesoftype);
      push_contiguous(reinterpret_cast<char*>(shuffleblock.data()), len);
    } else if(len > 0) {
      push_contiguous(data, len);
    }
  }
  
  void pushObj(SEXP const x, bool attributes_processed = false) {
    if(!attributes_processed && stypes.find(TYPEOF(x)) != stypes.end()) {
      std::vector<SEXP> anames;
      std::vector<SEXP> attrs;
      SEXP alist = ATTRIB(x);
      while(alist != R_NilValue) {
        anames.push_back(PRINTNAME(TAG(alist)));
        attrs.push_back(CAR(alist));
        alist = CDR(alist);
      }
      if(anames.size() != 0) {
        writeAttributeHeader_common(anames.size(), this);
        pushObj(x, true);
        for(uint64_t i=0; i<anames.size(); i++) {
          uint64_t alen = strlen(CHAR(anames[i]));
          writeStringHeader_common(alen,CE_NATIVE, this);
          push_contiguous(const_cast<char*>(CHAR(anames[i])), alen);
          pushObj(attrs[i]);
        }
      } else {
        pushObj(x, true);
      }
    } else if(TYPEOF(x) == STRSXP) {
      uint64_t dl = Rf_xlength(x);
      writeHeader_common(STRSXP, dl, this);
      for(uint64_t i=0; i<dl; i++) {
        SEXP xi = STRING_ELT(x, i);
        if(xi == NA_STRING) {
          push_noncontiguous(reinterpret_cast<char*>(const_cast<uint8_t*>(&string_header_NA)), 1);
        } else {
          uint64_t dl = LENGTH(xi);
          writeStringHeader_common(dl, Rf_getCharCE(xi), this);
          push_contiguous(const_cast<char*>(CHAR(xi)), dl);
        }
      }
    } else if(stypes.find(TYPEOF(x)) != stypes.end()) {
      uint64_t dl = Rf_xlength(x);
      writeHeader_common(TYPEOF(x), dl, this);
      if(TYPEOF(x) == VECSXP) {
        for(uint64_t i=0; i<dl; i++) {
          SEXP xi = VECTOR_ELT(x, i);
          pushObj(xi);
        }
      } else {
        switch(TYPEOF(x)) {
        case REALSXP:
          if(qm.real_shuffle) {
            shuffle_push(reinterpret_cast<char*>(REAL(x)), dl*8, 8);
          } else {
            push_contiguous(reinterpret_cast<char*>(REAL(x)), dl*8); 
          }
          break;
        case INTSXP:
          if(qm.int_shuffle) {
            shuffle_push(reinterpret_cast<char*>(INTEGER(x)), dl*4, 4); break;
          } else {
            push_contiguous(reinterpret_cast<char*>(INTEGER(x)), dl*4); 
          }
          break;
        case LGLSXP:
          if(qm.lgl_shuffle) {
            shuffle_push(reinterpret_cast<char*>(LOGICAL(x)), dl*4, 4); break;
          } else {
            push_contiguous(reinterpret_cast<char*>(LOGICAL(x)), dl*4); 
          }
          break;
        case RAWSXP:
          push_contiguous(reinterpret_cast<char*>(RAW(x)), dl); 
          break;
        case CPLXSXP:
          if(qm.cplx_shuffle) {
            shuffle_push(reinterpret_cast<char*>(COMPLEX(x)), dl*16, 8); break;
          } else {
            push_contiguous(reinterpret_cast<char*>(COMPLEX(x)), dl*16); 
          }
          break;
        case NILSXP:
          break;
        }
      }
    } else { // other non-supported SEXPTYPEs use the built in R serialization method
      Protect_Tracker pt = Protect_Tracker();
      SEXP xserialized = PROTECT(serializeToRaw(x)); pt++;
      uint64_t xs_size = Rf_xlength(xserialized);
      if(xs_size < 4294967296) {
        push_noncontiguous(reinterpret_cast<char*>(const_cast<uint8_t*>(&nstype_header_32)), 1);
        push_pod_contiguous(static_cast<uint32_t>(xs_size) );
      } else {
        push_noncontiguous(reinterpret_cast<char*>(const_cast<uint8_t*>(&nstype_header_64)), 1);
        push_pod_contiguous(static_cast<uint64_t>(xs_size) );
      }
      push_contiguous(reinterpret_cast<char*>(RAW(xserialized)), xs_size);
    }
  }
};


