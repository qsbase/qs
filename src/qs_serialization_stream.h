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

#include "qs_common.h"
#include "qs_serialize_common.h"

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
  void push(const char * const data, const uint64_t length) {
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
  void push(const char * const data, const uint64_t length) {
    if(qm.check_hash) xenv.update(data, length);
    bytes_written += length;
    write_check(con, data, length);
  }
};


template <class StreamClass> 
struct CompressBufferStream {
  QsMetadata qm;
  StreamClass & sobj;
  CountToObjectMap object_ref_hash;
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  std::vector<char> block = std::vector<char>(BLOCKSIZE);

  CompressBufferStream(StreamClass & so, QsMetadata qm) : qm(qm), sobj(so) {}
  inline void push_contiguous(const char * const data, uint64_t length) {
    sobj.push(data, length);
  }
  inline void push_noncontiguous(const char * const data, uint64_t length) {
    sobj.push(data, length);
  }
  template<typename POD>
  inline void push_pod_contiguous(const POD pod) {
    sobj.push(reinterpret_cast<const char * const>(&pod), sizeof(pod));
  }
  template<typename POD>
  inline void push_pod_noncontiguous(const POD pod) {
    sobj.push(reinterpret_cast<const char * const>(&pod), sizeof(pod));
  }
  // template<typename POD>
  // inline void push_pod_noncontiguous(const POD pod1, const POD pod2) {
  //   sobj.push(reinterpret_cast<const char * const>(&pod1), sizeof(pod1)); 
  //   sobj.push(reinterpret_cast<const char * const>(&pod2), sizeof(pod2));
  // }
  void shuffle_push(const char * const data, const uint64_t len, const uint64_t bytesoftype) {
    if(len > MIN_SHUFFLE_ELEMENTS) {
      if(len > shuffleblock.size()) shuffleblock.resize(len);
      blosc_shuffle(reinterpret_cast<const uint8_t * const>(data), shuffleblock.data(), len, bytesoftype);
      sobj.push(reinterpret_cast<char*>(shuffleblock.data()), len);
    } else if(len > 0) {
      sobj.push(data, len);
    }
  }
};
