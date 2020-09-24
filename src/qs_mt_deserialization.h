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

// struct Ordered_Counter {
//   uint64_t cached_counter_val;
//   std::atomic<uint64_t> counter;
//   std::vector< std::atomic<uint64_t> > thread_counters;
//   Ordered_Counter(unsigned int nthreads) : cached_counter_val(0), counter(0), thread_counters(nthreads) {
//     for(unsigned int i=0; i<nthreads; i++) {
//       thread_counters[i] = 0;
//     }
//   }
//   void update_counter(unsigned int thread_id) {
//     thread_counters[thread_id]++;
//   }
//   bool check_lte(uint64_t val) {
//     if(cached_counter_val > val) return false;
//     auto it = std::min_element(thread_counters.begin(), thread_counters.end());
//     uint64_t min_val = *it;
//     uint64_t first_min = it - thread_counters.begin();
//     cached_counter_val = min_val * thread_counters.size() + first_min;
//     return cached_counter_val <= val;
//   }
//   bool check_lt(uint64_t val) {
//     if(cached_counter_val >= val) return false;
//     auto it = std::min_element(thread_counters.begin(), thread_counters.end());
//     uint64_t min_val = *it;
//     uint64_t first_min = it - thread_counters.begin();
//     cached_counter_val = min_val * thread_counters.size() + first_min;
//     return cached_counter_val < val;
//   }
// };

template <class decompress_env> 
struct Data_Thread_Context {
  std::ifstream & myFile;
  decompress_env denv; // default constructor
  const unsigned int nthreads;
  
  uint64_t blocks_total;
  std::atomic<uint64_t> blocks_read;
  std::atomic<uint64_t>  blocks_processed;
  
  std::vector<bool> primary_block = std::vector<bool>(nthreads, true);
  std::vector< std::vector<char> > zblocks; // one per thread
  std::vector< std::vector<char> > data_blocks; // one per thread
  std::vector< std::vector<char> > data_blocks2; // one per thread
  std::pair<char*, uint64_t> data_pass; // default constructor
  
  std::vector< std::atomic<char*> > block_pointers;
  std::vector< std::atomic<uint64_t> > block_sizes;
  std::vector< std::atomic<uint8_t> > data_task;
  std::vector<std::thread> threads;

  Data_Thread_Context(std::ifstream & mf, unsigned int nt, QsMetadata qm) : 
    myFile(mf), nthreads(nt), blocks_total(qm.clength), blocks_read(0), blocks_processed(0),
    zblocks(std::vector< std::vector<char> >(nt, std::vector<char>(this->denv.compressBound(BLOCKSIZE)))),
    data_blocks(std::vector< std::vector<char> >(nt, std::vector<char>(BLOCKSIZE))),
    data_blocks2(std::vector<std::vector<char> >(nt, std::vector<char>(BLOCKSIZE))) {
    block_pointers = std::vector< std::atomic<char*> >(nt);
    for(unsigned int i=0; i<nt; i++) {
      block_pointers[i] = nullptr;
    }
    block_sizes = std::vector< std::atomic<uint64_t> >(nt);
    for(unsigned int i=0; i<nt; i++) {
      block_sizes[i] = 0;
    }
    data_task = std::vector< std::atomic<uint8_t> >(nt);
    for(unsigned int i=0; i<nt; i++) {
      data_task[i] = 0;
    }
    for (unsigned int i = 0; i < nt; i++) {
      threads.push_back(std::thread(&Data_Thread_Context::worker_thread, this, i));
    }
  }
  
  void worker_thread(unsigned int thread_id) {
    std::array<char,4> zsize_ar;
    for(uint64_t i=thread_id; i < blocks_total; i += nthreads) {
      // tout << thread_id << " " << i <<  "begin\n" << std::flush;
      while(blocks_read != i) {
        std::this_thread::yield();
      }
      myFile.read(zsize_ar.data(), 4);
      uint32_t zsize = unaligned_cast<uint32_t>(zsize_ar.data(),0);
      myFile.read(zblocks[thread_id].data(), zsize);
      blocks_read++;
      
      // task marching orders from main thread
      // 0 = wait
      // 1 = nothing (main thread will use block as is)
      // 2 = memcpy
      // it seems to be slower to check for task order and direct decompress
      // rather than just assuming we should memcpy
      // if(data_task[thread_id] == 2) {
      //   char* dp = data_pass.first;
      //   data_task[thread_id] = 0;
      //   decompFun(dp, BLOCKSIZE, zblocks[thread_id].data(), zsize);
      // } else {
      if(primary_block[thread_id]) {
        block_sizes[thread_id] = denv.decompress(data_blocks[thread_id].data(), BLOCKSIZE, zblocks[thread_id].data(), zsize);
        block_pointers[thread_id] = data_blocks[thread_id].data();
      } else {
        block_sizes[thread_id] = denv.decompress(data_blocks2[thread_id].data(), BLOCKSIZE, zblocks[thread_id].data(), zsize);
        block_pointers[thread_id] = data_blocks2[thread_id].data();
      }
      while(data_task[thread_id] == 0) {
        std::this_thread::yield();
      }
      if(data_task[thread_id] == 1) {
        data_pass.first = block_pointers[thread_id];
        data_pass.second = block_sizes[thread_id];
        data_task[thread_id] = 0;
      } else { // data task == 2
        char* dp = data_pass.first;
        std::memcpy(dp, block_pointers[thread_id], block_sizes[thread_id]);
        data_task[thread_id] = 0;
      }
      // }
      
      primary_block[thread_id] = !primary_block[thread_id];
    }
    // tout << thread_id << " finished thread for loop\n" << std::flush;
  }
  
  void finish() {
    blocks_processed++;
    for(unsigned int i=0; i < nthreads; i++) {
      // tout << "join called " << i << "\n" << std::flush;
      threads[i].join();
    }
  }
  
  std::pair<char*, uint64_t> get_block_ptr() {
    uint64_t current_block = blocks_processed % nthreads;
    blocks_processed++;
    while(data_task[current_block] != 0) std::this_thread::yield();
    data_task[current_block] = 1;
    while(data_task[current_block] != 0) std::this_thread::yield();
    char* temp_ptr = data_pass.first;
    uint64_t temp_size = data_pass.second;
    return std::pair<char*, uint64_t>(temp_ptr, temp_size);
  }
  
  void decompress_data_direct(char* bpointer) {
    uint64_t current_block = blocks_processed % nthreads;
    blocks_processed++;
    while(data_task[current_block] != 0) std::this_thread::yield();
    data_pass.first = bpointer;
    data_task[current_block] = 2;
    while(data_task[current_block] != 0) std::this_thread::yield();
  }
};

template <class decompress_env> 
struct Data_Context_MT {
  QsMetadata qm;
  std::ifstream & myFile;
  Data_Thread_Context<decompress_env> dtc;
  xxhash_env xenv; // default constructoer
  std::unordered_map<uint32_t, SEXP> object_ref_hash;
  bool use_alt_rep_bool;
  
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  char* block_data; // default constructor
  uint64_t block_size = 0;
  uint64_t data_offset = 0;
  std::string temp_string = std::string(256, '\0');
  
  Data_Context_MT(std::ifstream & mf, QsMetadata qm, bool use_alt_rep, unsigned int nthreads) : 
    qm(qm), myFile(mf), dtc(mf, nthreads-1, qm), use_alt_rep_bool(use_alt_rep) {}
  void readHeader(qstype & object_type, uint64_t & r_array_len) {
    if(data_offset >= block_size) decompress_block();
    char* header = block_data;
    readHeader_common(object_type, r_array_len, data_offset, header);
  }
  void readStringHeader(uint32_t & r_string_len, cetype_t & ce_enc) {
    if(data_offset >= block_size) decompress_block();
    char* header = block_data;
    readStringHeader_common(r_string_len, ce_enc, data_offset, header);
  }
  void readFlags(int & packed_flags) {
    if(data_offset >= block_size) decompress_block();
    char* header = block_data;
    readFlags_common(packed_flags, data_offset, header);
  }
  void decompress_direct(char* bpointer) {
    dtc.decompress_data_direct(bpointer);
    if(qm.check_hash) xenv.update(bpointer, BLOCKSIZE);
  }
  void decompress_block() {
    auto res = dtc.get_block_ptr();
    block_data = res.first;
    block_size = res.second;
    data_offset = 0;
    if(qm.check_hash) xenv.update(block_data, block_size);
    // tout << "main thread decompress block " << (void *)block_data << " " << block_size << "\n" << std::flush;
  }
  void getBlockData(char* outp, uint64_t data_size) {
    // tout << "main thread get block data " << data_size << " " << block_size << " " << data_offset << "\n" << std::flush;
    if(data_size <= block_size - data_offset) {
      std::memcpy(outp, block_data+data_offset, data_size);
      data_offset += data_size;
    } else {
      uint64_t bytes_accounted = block_size - data_offset;
      std::memcpy(outp, block_data+data_offset, bytes_accounted);
      while(bytes_accounted < data_size) {
        if(data_size - bytes_accounted >= BLOCKSIZE) {
          decompress_direct(outp+bytes_accounted);
          bytes_accounted += BLOCKSIZE;
          data_offset = BLOCKSIZE;
        } else {
          decompress_block();
          std::memcpy(outp + bytes_accounted, block_data, data_size - bytes_accounted);
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