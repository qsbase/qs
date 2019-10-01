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

// built in zstd streaming context
template <class stream_writer>
struct ZSTD_streamWrite {
  QsMetadata qm;
  stream_writer & myFile;
  xxhash_env xenv;
  uint64_t bytes_written = 0;
  std::vector<char> outblock = std::vector<char>(ZSTD_CStreamOutSize());
  ZSTD_inBuffer zin;
  ZSTD_outBuffer zout;
  ZSTD_CStream* zcs;
  ZSTD_streamWrite(stream_writer & mf, QsMetadata qm) : qm(qm), myFile(mf) {
    zcs = ZSTD_createCStream();
    ZSTD_initCStream(zcs, qm.compress_level);
    zout.size = ZSTD_CStreamOutSize();
    zout.pos = 0;
    zout.dst = outblock.data();
  }
  ~ZSTD_streamWrite() {
    ZSTD_freeCStream(zcs);
  }
  void push_contiguous(const char * const data, const uint64_t length) {
    if(qm.check_hash) xenv.update(data, length);
    zin.pos = 0;
    zin.src = data;
    zin.size = length;
    bytes_written += zin.size;
    while(zin.pos < zin.size) {
      zout.pos = 0;
      uint64_t return_value = ZSTD_compressStream(zcs, &zout, &zin);
      if(ZSTD_isError(return_value)) throw std::runtime_error("zstd stream compression error; output is likely corrupted");
      if(zout.pos > 0) write_check(myFile, reinterpret_cast<char*>(zout.dst), zout.pos);
    }
  }
  inline void push_noncontiguous(const char * const data, uint64_t length) {
    push_contiguous(data, length);
  }
  template<typename POD>
  void push_pod_contiguous(const POD pod) {
    push_contiguous(reinterpret_cast<const char * const>(&pod), sizeof(pod));
  }
  template<typename POD>
  inline void push_pod_noncontiguous(const POD pod) {
    push_contiguous(reinterpret_cast<const char * const>(&pod), sizeof(pod));
  }
  
  void flush() {
    uint64_t remain;
    do {
      zout.pos = 0;
      remain = ZSTD_flushStream(zcs, &zout);
      if(ZSTD_isError(remain)) throw std::runtime_error("zstd stream compression error; output is likely corrupted");
      if(zout.pos > 0) myFile.write(reinterpret_cast<char*>(zout.dst), zout.pos);
    } while (remain != 0);
  }
};

// #ifdef USE_R_CONNECTION
// // Rconnection context
// struct rconn_streamWrite {
//   Rconnection con;
//   xxhash_env xenv;
//   QsMetadata qm;
//   rconn_streamWrite(Rconnection _con, QsMetadata qm) : con(_con), xenv(xxhash_env()), qm(qm) {}
//   void push(char * data, uint64_t length) {
//     if(qm.check_hash) xenv.update(data, length);
//     fwrite_check(data, length, con);
//   }
//   template<typename POD>
//   void push_pod(POD pod) {
//     push(reinterpret_cast<char*>(&pod), sizeof(pod));
//   }
// };
// #endif

template <class stream_writer>
struct uncompressed_streamWrite {
  QsMetadata qm;
  stream_writer & con;
  xxhash_env xenv;
  uint64_t bytes_written = 0;
  uncompressed_streamWrite(stream_writer & _con, QsMetadata qm) : qm(qm), con(_con) {}
  void push_contiguous(const char * const data, const uint64_t length) {
    if(qm.check_hash) xenv.update(data, length);
    bytes_written += length;
    write_check(con, data, length);
  }
  inline void push_noncontiguous(const char * const data, const uint64_t length) {
    push_contiguous(data, length);
  }
  template<typename POD>
  void push_pod_contiguous(const POD pod) {
    push_contiguous(reinterpret_cast<const char * const>(&pod), sizeof(pod));
  }
  template<typename POD>
  void push_pod_noncontiguous(POD pod) {
    push_contiguous(reinterpret_cast<const char * const>(&pod), sizeof(pod));
  }
};


template <class StreamClass> 
struct CompressBufferStream {
  QsMetadata qm;
  StreamClass & sobj;
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  std::vector<char> block = std::vector<char>(BLOCKSIZE);

  CompressBufferStream(StreamClass & so, QsMetadata qm) : qm(qm), sobj(so) {}
  void shuffle_push(const char * const data, const uint64_t len, const uint64_t bytesoftype) {
    if(len > MIN_SHUFFLE_ELEMENTS) {
      if(len > shuffleblock.size()) shuffleblock.resize(len);
      blosc_shuffle(reinterpret_cast<const uint8_t * const>(data), shuffleblock.data(), len, bytesoftype);
      sobj.push_contiguous(reinterpret_cast<char*>(shuffleblock.data()), len);
    } else if(len > 0) {
      sobj.push_contiguous(data, len);
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
        writeAttributeHeader_common(anames.size(), &sobj);
        pushObj(x, true);
        for(uint64_t i=0; i<anames.size(); i++) {
          uint64_t alen = strlen(CHAR(anames[i]));
          writeStringHeader_common(alen,CE_NATIVE, &sobj);
          sobj.push_contiguous(CHAR(anames[i]), alen);
          pushObj(attrs[i]);
        }
      } else {
        pushObj(x, true);
      }
    } else if(TYPEOF(x) == STRSXP) {
      uint64_t dl = Rf_xlength(x);
      writeHeader_common(STRSXP, dl, &sobj);
      for(uint64_t i=0; i<dl; i++) {
        SEXP xi = STRING_ELT(x, i);
        if(xi == NA_STRING) {
          sobj.push_contiguous(reinterpret_cast<const char *>(&string_header_NA), 1);
        } else {
          uint64_t dl = LENGTH(xi);
          writeStringHeader_common(dl, Rf_getCharCE(xi), &sobj);
          sobj.push_contiguous(CHAR(xi), dl);
        }
      }
    } else if(stypes.find(TYPEOF(x)) != stypes.end()) {
      uint64_t dl = Rf_xlength(x);
      writeHeader_common(TYPEOF(x), dl, &sobj);
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
            sobj.push_contiguous(reinterpret_cast<char*>(REAL(x)), dl*8); 
          }
          break;
        case INTSXP:
          if(qm.int_shuffle) {
            shuffle_push(reinterpret_cast<char*>(INTEGER(x)), dl*4, 4); break;
          } else {
            sobj.push_contiguous(reinterpret_cast<char*>(INTEGER(x)), dl*4); 
          }
          break;
        case LGLSXP:
          if(qm.lgl_shuffle) {
            shuffle_push(reinterpret_cast<char*>(LOGICAL(x)), dl*4, 4); break;
          } else {
            sobj.push_contiguous(reinterpret_cast<char*>(LOGICAL(x)), dl*4); 
          }
          break;
        case RAWSXP:
          sobj.push_contiguous(reinterpret_cast<char*>(RAW(x)), dl); 
          break;
        case CPLXSXP:
          if(qm.cplx_shuffle) {
            shuffle_push(reinterpret_cast<char*>(COMPLEX(x)), dl*16, 8); break;
          } else {
            sobj.push_contiguous(reinterpret_cast<char*>(COMPLEX(x)), dl*16); 
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
        sobj.push_noncontiguous(reinterpret_cast<const char *>(&nstype_header_32), 1);
        sobj.push_pod_contiguous(static_cast<uint32_t>(xs_size));
      } else {
        sobj.push_noncontiguous(reinterpret_cast<const char *>(&nstype_header_64), 1);
        sobj.push_pod_contiguous(static_cast<uint64_t>(xs_size));
      }
      sobj.push_contiguous(reinterpret_cast<char*>(RAW(xserialized)), xs_size);
    }
  }
};
