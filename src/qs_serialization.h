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
#include "qs_serialize_common.h"

////////////////////////////////////////////////////////////////
// serialization functions
////////////////////////////////////////////////////////////////

template <class stream_writer, class compress_env> 
struct CompressBuffer {
  QsMetadata qm;
  stream_writer & myFile;
  compress_env cenv; // default constructor
  xxhash_env xenv; // default constructor
  CountToObjectMap object_ref_hash; // default constructor
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
  // template<typename POD>
  // inline void push_pod_noncontiguous(const POD pod1, const POD pod2) {
  //  std::array<char, sizeof(POD)*2> pdata = {};
  //  std::memcpy(pdata.data(), reinterpret_cast<const char*>(&pod1), sizeof(POD));
  //  std::memcpy(pdata.data() + sizeof(POD), reinterpret_cast<const char*>(&pod2), sizeof(POD));
  //  push_noncontiguous(pdata.data(), sizeof(POD)*2);
  //}
  void shuffle_push(const char * const data, const uint64_t len, const uint64_t bytesoftype) {
    if(len > MIN_SHUFFLE_ELEMENTS) {
      if(len > shuffleblock.size()) shuffleblock.resize(len);
      blosc_shuffle(reinterpret_cast<const uint8_t * const>(data), shuffleblock.data(), len, bytesoftype);
      push_contiguous(reinterpret_cast<char*>(shuffleblock.data()), len);
    } else if(len > 0) {
      push_contiguous(data, len);
    }
  }
};


