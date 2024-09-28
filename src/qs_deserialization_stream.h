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
#include "qs_deserialize_common.h"

static constexpr uint64_t RESERVE_SIZE = 4;

template <class stream_reader>
struct ZSTD_streamRead {
  QsMetadata qm;
  stream_reader & myFile;
  xxhash_env xenv; // default constructor
  uint64_t minblocksize = ZSTD_DStreamOutSize();
  uint64_t maxblocksize = 4 * ZSTD_DStreamOutSize();
  uint64_t decompressed_bytes_read = 0;
  uint64_t compressed_bytes_read = 0;
  std::vector<char> outblock = std::vector<char>(maxblocksize);
  std::vector<char> inblock = std::vector<char>(ZSTD_DStreamInSize());
  uint64_t blocksize = 0; // shared with Data_Context_Stream by reference -- block_size
  uint64_t blockoffset = 0; // shared with Data_Context_Stream by reference -- data_offset
  ZSTD_inBuffer zin;
  ZSTD_outBuffer zout;
  ZSTD_DStream* zds;
  std::array<char, 4> hash_reserve;
  bool end_of_decompression = false;

  ZSTD_streamRead(stream_reader & mf, QsMetadata qm) :
    qm(qm), myFile(mf) {
    zds = ZSTD_createDStream();
    ZSTD_initDStream(zds);
    zout.size = maxblocksize;
    zout.pos = 0;
    zout.dst = outblock.data();
    zin.size = 0;
    zin.pos = 0;
    zin.src = inblock.data();
    if(qm.check_hash) {
      read_check(myFile, hash_reserve.data(), RESERVE_SIZE);
    }
  }

  size_t read_reserve(char * dst, size_t length, bool exact=false) {
    if(!qm.check_hash) {
      size_t bytes_out;
      if(exact) {
        bytes_out = read_check(myFile, dst, length);
      } else {
        bytes_out = read_allow(myFile, dst, length);
      }
      // compressed_bytes_read += bytes_out;
      // std::cout << "compressed bytes read: " << compressed_bytes_read << std::endl;
      return bytes_out;
    }
    if(exact) {
      if(length >= RESERVE_SIZE) {
        std::memcpy(dst, hash_reserve.data(), RESERVE_SIZE);
        read_check(myFile, dst + RESERVE_SIZE, length - RESERVE_SIZE);
        read_check(myFile, hash_reserve.data(), RESERVE_SIZE);
      } else { // RESERVE_SIZE > length
        std::memcpy(dst, hash_reserve.data(), length);
        // since some of the reserve buffer was consumed, shift the unconsumed bytes to beginning of array
        // then read from file to fill up reserve buffer
        std::memmove(hash_reserve.data(), hash_reserve.data() + length, RESERVE_SIZE - length);
        read_check(myFile, hash_reserve.data() +  RESERVE_SIZE - length, length);
      }

      // for(unsigned int i=0; i < 4; i++) std::cout << std::hex << (int)reinterpret_cast<uint8_t &>(hash_reserve.data()[i]) << " "; std::cout << std::endl;
      return length;
    } else { // !exact -- we can't assume that "length" bytes are left in myFile; there could even be zero bytes left
      if(length >= RESERVE_SIZE) {
        // use "dst" as a temporary buffer, since it's already allocated
        // it is not a good idea to allocate a temp buffer of size length, as length can be large
        std::memcpy(dst, hash_reserve.data(), RESERVE_SIZE);
        size_t n_read = read_allow(myFile, dst + RESERVE_SIZE, length - RESERVE_SIZE);
        size_t n_bufferable = n_read + RESERVE_SIZE;
        if(n_bufferable < length) {
          std::memcpy(hash_reserve.data(), dst + n_bufferable - RESERVE_SIZE, RESERVE_SIZE);

          // for(unsigned int i=0; i < 4; i++) std::cout << std::hex << (int)reinterpret_cast<uint8_t &>(hash_reserve.data()[i]) << " "; std::cout << std::endl;
          return n_bufferable - RESERVE_SIZE;
        } else {
          std::array<char, RESERVE_SIZE> temp_buffer;
          size_t temp_size = read_allow(myFile, temp_buffer.data(), RESERVE_SIZE);
          std::memcpy(hash_reserve.data(), dst + n_bufferable - (RESERVE_SIZE - temp_size), RESERVE_SIZE - temp_size);
          std::memcpy(hash_reserve.data() + RESERVE_SIZE - temp_size, temp_buffer.data(), temp_size);

          // for(unsigned int i=0; i < 4; i++) std::cout << std::hex << (int)reinterpret_cast<uint8_t &>(hash_reserve.data()[i]) << " "; std::cout << std::endl;
          return n_bufferable - (RESERVE_SIZE - temp_size);
        }
      } else { // length < RESERVE_SIZE
        std::vector<char> temp_buffer(length, '\0');
        size_t return_value = read_allow(myFile, temp_buffer.data(), length);
        // n_bufferable is at most RESERVE_SIZE*2 - 1 = 7
        std::memcpy(dst, hash_reserve.data(), return_value);
        std::memmove(hash_reserve.data(), hash_reserve.data() + return_value, RESERVE_SIZE - return_value);
        std::memcpy(hash_reserve.data() + (RESERVE_SIZE - return_value), temp_buffer.data(), return_value);

        // for(unsigned int i=0; i < 4; i++) std::cout << std::hex << (int)reinterpret_cast<uint8_t &>(hash_reserve.data()[i]) << " "; std::cout << std::endl;
        return return_value;
      }
    }
  }
  ~ZSTD_streamRead() {
    ZSTD_freeDStream(zds);
  }
  inline uint64_t ZSTD_decompressStream_count(ZSTD_DStream* zds, ZSTD_outBuffer * zout, ZSTD_inBuffer * zin) {
    uint64_t temp = zout->pos;
    size_t return_value = ZSTD_decompressStream(zds, zout, zin);
    if(ZSTD_isError(return_value)) throw std::runtime_error("zstd stream decompression error");
    decompressed_bytes_read += zout->pos - temp;
    // std::cout << "decomp: " << zout->pos - temp << std::endl;
    xenv.update(reinterpret_cast<char*>(zout->dst)+temp, zout->pos - temp);
    return zout->pos - temp;
    // for(unsigned int i=0; i < zout->pos - temp; i++) std::cout << std::hex << (int)(reinterpret_cast<uint8_t *>(zout->dst)[i+temp]) << " "; std::cout << std::endl;
    // std::cout << std::dec << xenv.digest() << std::endl;
  }
  void getBlock() {
    // std::cout << "getblock: offset: " << blockoffset << ", blocksize: " << blocksize << ", bytes_read: " << decompressed_bytes_read << std::endl;
    if(end_of_decompression) return;
    // if((qm.clength != 0) & (decompressed_bytes_read >= qm.clength)) return;
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
      } else {
        uint64_t bytes_read = read_reserve(inblock.data(), inblock.size(), false);
        zin.pos = 0;
        zin.size = bytes_read;
        uint64_t bytes_decompressed = ZSTD_decompressStream_count(zds, &zout, &zin);
        if(bytes_read == 0 && bytes_decompressed == 0) {
          end_of_decompression = true;
          break;
        }
      }
    }
    blocksize = zout.pos;
    blockoffset = 0;
  }
  void copyData(char* dst, uint64_t dst_size) {
    // std::cout << "copydata: offset: " << blockoffset << ", blocksize: " << blocksize << ", bytes_read: " << decompressed_bytes_read << std::endl;
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
        } else {
          uint64_t bytes_read = read_reserve(inblock.data(), inblock.size(), false);
          // std::cout << "zin.pos >= zin.size: " << zin.pos << "/" << zin.size << " zin " << zout.pos << "/" << zout.size << " zout\n";
          zin.pos = 0;
          zin.size = bytes_read;
          ZSTD_decompressStream_count(zds, &zout, &zin);
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
  QsMetadata qm;
  stream_reader & con;
  std::vector<char> outblock = std::vector<char>(BLOCKSIZE+BLOCKRESERVE);
  uint64_t blocksize = 0; // shared with Data_Context_Stream by reference -- block_size
  uint64_t blockoffset = 0; // shared with Data_Context_Stream by reference -- data_offset
  uint64_t decompressed_bytes_read = 0; // same as total bytes read since no compression
  xxhash_env xenv; // default constructor
  std::array<char, 4> hash_reserve;
  uncompressed_streamRead(stream_reader & _con, QsMetadata qm) :
    qm(qm), con(_con) {
    if(qm.check_hash) {
      read_check(con, hash_reserve.data(), RESERVE_SIZE);
    }
  }

  // fread with updating hash as necessary
  size_t read_update(char * dst, size_t length, bool exact=false) {
    if(!qm.check_hash) {
      size_t return_value;
      if(exact) {
        return_value = read_check(con, dst, length);
      } else {
        return_value = read_allow(con, dst, length);
      }
      decompressed_bytes_read += return_value;
      xenv.update(dst, return_value);
      return return_value;
    }
    if(exact) {
      if(length >= RESERVE_SIZE) {
        std::memcpy(dst, hash_reserve.data(), RESERVE_SIZE);
        read_check(con, dst + RESERVE_SIZE, length - RESERVE_SIZE);
        read_check(con, hash_reserve.data(), RESERVE_SIZE);
      } else { // RESERVE_SIZE > length
        std::memcpy(dst, hash_reserve.data(), length);
        // since some of the reserve buffer was consumed, shift the unconsumed bytes to beginning of array
        // then read from file to fill up reserve buffer
        std::memmove(hash_reserve.data(), hash_reserve.data() + length, RESERVE_SIZE - length);
        read_check(con, hash_reserve.data() +  RESERVE_SIZE - length, length);
      }
      decompressed_bytes_read += length;
      xenv.update(dst, length);
      return length;
    } else { // !exact -- we can't assume that "length" bytes are left in myFile; there could even be zero bytes left
      if(length >= RESERVE_SIZE) {
        // use "dst" as a temporary buffer, since it's already allocated
        // it is not a good idea to allocate a temp buffer of size length, as length can be large
        std::memcpy(dst, hash_reserve.data(), RESERVE_SIZE);
        size_t n_read = read_allow(con, dst + RESERVE_SIZE, length - RESERVE_SIZE);
        size_t n_bufferable = n_read + RESERVE_SIZE;
        if(n_bufferable < length) {
          std::memcpy(hash_reserve.data(), dst + n_bufferable - RESERVE_SIZE, RESERVE_SIZE);
          decompressed_bytes_read += n_bufferable - RESERVE_SIZE;
          xenv.update(dst, n_bufferable - RESERVE_SIZE);
          return n_bufferable - RESERVE_SIZE;
        } else {
          std::array<char, RESERVE_SIZE> temp_buffer;
          size_t temp_size = read_allow(con, temp_buffer.data(), RESERVE_SIZE);
          std::memcpy(hash_reserve.data(), dst + n_bufferable - (RESERVE_SIZE - temp_size), RESERVE_SIZE - temp_size);
          std::memcpy(hash_reserve.data() + RESERVE_SIZE - temp_size, temp_buffer.data(), temp_size);
          decompressed_bytes_read += n_bufferable - (RESERVE_SIZE - temp_size);
          xenv.update(dst, n_bufferable - (RESERVE_SIZE - temp_size));
          return n_bufferable - (RESERVE_SIZE - temp_size);
        }
      } else { // length < RESERVE_SIZE
        std::vector<char> temp_buffer(length, '\0');
        size_t return_value = read_allow(con, temp_buffer.data(), length);
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
  QsMetadata qm;
  DestreamClass & dsc;
  bool use_alt_rep_bool;
  std::unordered_map<uint32_t, SEXP> object_ref_hash;
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  uint64_t & data_offset; // dsc.blockoffset
  uint64_t & block_size; // dsc.blocksize
  char * data_ptr;

  Data_Context_Stream(DestreamClass & d, QsMetadata q, bool use_alt_rep) : qm(q), dsc(d), use_alt_rep_bool(use_alt_rep),
    shuffleblock(std::vector<uint8_t>(256)), data_offset(d.blockoffset), block_size(d.blocksize), data_ptr(d.outblock.data()) {}

  void getBlock() {
    dsc.getBlock();
  }
  void getBlockData(char* outp, uint64_t data_size) {
    dsc.copyData(outp, data_size);
  }
  void readHeader(qstype & object_type, uint64_t & r_array_len) {
    if(data_offset + BLOCKRESERVE >= block_size) getBlock();
    readHeader_common(object_type, r_array_len, data_offset, data_ptr);
  }
  void readStringHeader(uint32_t & r_string_len, cetype_t & ce_enc) {
    if(data_offset + BLOCKRESERVE >= block_size) getBlock();
    readStringHeader_common(r_string_len, ce_enc, data_offset, data_ptr);
  }
  void readFlags(int & packed_flags) {
    if(data_offset + BLOCKRESERVE >= block_size) getBlock();
    readFlags_common(packed_flags, data_offset, data_ptr);
  }
  char * tempBlock(uint64_t data_size) {
    if(data_size > shuffleblock.size()) shuffleblock.resize(data_size);
    return reinterpret_cast<char*>(shuffleblock.data());
  }
  char * tempBlock() {
    return reinterpret_cast<char*>(shuffleblock.data());
  }
  std::string getString(uint64_t data_size) {
    std::string temp_string;
    temp_string.resize(data_size);
    getBlockData(&temp_string[0], data_size);
    return temp_string;
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
};

