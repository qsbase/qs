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
#include "qs_deserialize_common.h"

////////////////////////////////////////////////////////////////
// de-serialization functions
////////////////////////////////////////////////////////////////

template <class stream_reader, class decompress_env> 
struct Data_Context {
  QsMetadata qm;
  stream_reader & myFile;
  bool use_alt_rep_bool;
  
  decompress_env denv; // default constructor
  xxhash_env xenv; // default constructor
  std::unordered_map<uint32_t, SEXP> object_ref_hash;
  
  std::vector<char> zblock = std::vector<char>(denv.compressBound(BLOCKSIZE));
  std::vector<char> block = std::vector<char>(BLOCKSIZE);
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  uint64_t data_offset = 0;
  uint64_t blocks_read = 0;
  uint64_t block_size = 0;
  std::string temp_string = std::string(256, '\0');
  
  Data_Context(stream_reader & mf, QsMetadata qm, bool use_alt_rep) : 
    qm(qm), myFile(mf), use_alt_rep_bool(use_alt_rep) {}
  
  void readHeader(qstype & object_type, uint64_t & r_array_len) {
    if(data_offset >= block_size) decompress_block();
    char* header = block.data();
    readHeader_common(object_type, r_array_len, data_offset, header);
  }
  void readStringHeader(uint32_t & r_string_len, cetype_t & ce_enc) {
    if(data_offset >= block_size) decompress_block();
    char* header = block.data();
    readStringHeader_common(r_string_len, ce_enc, data_offset, header);
  }
  void readFlags(int & packed_flags) {
    if(data_offset >= block_size) decompress_block();
    char* header = block.data();
    readFlags_common(packed_flags, data_offset, header);
  }
  void decompress_direct(char* bpointer) {
    blocks_read++;
    std::array<char, 4> zsize_ar;
    read_allow(myFile, zsize_ar.data(), 4);
    uint64_t zsize = *reinterpret_cast<uint32_t*>(zsize_ar.data());
    read_allow(myFile, zblock.data(), zsize);
    block_size = denv.decompress(bpointer, BLOCKSIZE, zblock.data(), zsize);
    if(qm.check_hash) xenv.update(bpointer, BLOCKSIZE);
  }
  void decompress_block() {
    blocks_read++;
    std::array<char, 4> zsize_ar;
    // uint64_t bytes_read = read_allow(myFile, zsize_ar.data(), 4);
    // if(bytes_read == 0) return;
    read_allow(myFile, zsize_ar.data(), 4);
    uint64_t zsize = *reinterpret_cast<uint32_t*>(zsize_ar.data());
    read_allow(myFile, zblock.data(), zsize);
    block_size = denv.decompress(block.data(), BLOCKSIZE, zblock.data(), zsize);
    data_offset = 0;
    if(qm.check_hash) xenv.update(block.data(), block_size);
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
          data_offset = BLOCKSIZE;
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
};
