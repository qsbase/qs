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

#include <iostream>
#include <sstream>
#include <mutex>


// #define QS_MT_SERIALIZATION_DEBUG

#ifdef QS_MT_DESERIALIZATION_DEBUG
// https://stackoverflow.com/a/53288135/2723734
// Thread-safe std::ostream class.

#define terr ThreadStream(std::cerr)
#define tout ThreadStream(std::cout)
class ThreadStream : public std::ostringstream
{
public:
  ThreadStream(std::ostream& os) : os_(os)
  {
    imbue(os.getloc());
    precision(os.precision());
    width(os.width());
    setf(std::ios::fixed, std::ios::floatfield);
  }
  
  ~ThreadStream()
  {
    std::lock_guard<std::mutex> guard(_mutex_threadstream);
    os_ << this->str();
  }
  
private:
  static std::mutex _mutex_threadstream;
  std::ostream& os_;
};

std::mutex ThreadStream::_mutex_threadstream{};

#endif


////////////////////////////////////////////////////////////////
// multi-thread serialization functions
////////////////////////////////////////////////////////////////

template <class compress_env> 
struct Compress_Thread_Context {
  std::ofstream* myFile;
  compress_env cenv;
  
  std::atomic<uint64_t> blocks_total;
  std::atomic<uint64_t> blocks_written;
  
  unsigned int nthreads;
  int compress_level;  
  std::atomic<bool> done;
  
  std::vector<std::vector<char> > zblocks; // one per thread
  std::vector<std::vector<char> > data_blocks; // one per thread
  std::vector< std::pair<const char*, uint64_t> > block_pointers;
  
  std::vector< std::atomic<bool> > data_ready;
  std::vector<std::thread> threads;
  
  void worker_thread(unsigned int thread_id) {
    while(!done) {
      // check if data ready and then compress

      // tout << "waiting on data " << blocks_written << " thread " << thread_id << "\n" << std::flush;

      while (!data_ready[thread_id]) {
        std::this_thread::yield();
        if(done) break;
      }; if(done) break;
      
      uint64_t zsize = cenv.compress(zblocks[thread_id].data(), zblocks[thread_id].size(), block_pointers[thread_id].first, block_pointers[thread_id].second, compress_level);
      data_ready[thread_id] = false;

      // tout << "data ready to write " << blocks_written << " thread " << thread_id << "\n" << std::flush;

      // write to file
      while (blocks_written % nthreads != thread_id) {
        std::this_thread::yield();
      }
      writeSize4(*myFile, zsize);
      myFile->write(zblocks[thread_id].data(), zsize);
      blocks_written += 1;

      // tout << "blocks written " << blocks_written << " thread " << thread_id << "\n" << std::flush;

    }

    // tout << "exit main loop " << blocks_written << " thread " << thread_id << "\n" << std::flush;

    
    // final check to see if any remaining data
    if(data_ready[thread_id]) {
      uint64_t zsize = cenv.compress(zblocks[thread_id].data(), zblocks[thread_id].size(), block_pointers[thread_id].first, block_pointers[thread_id].second, compress_level);

      // tout << "final data ready to write " << blocks_written << " thread " << thread_id << "\n" << std::flush;

      // write to file
      while (blocks_written % nthreads != thread_id) {
        std::this_thread::yield();
      }
      writeSize4(*myFile, zsize);
      myFile->write(zblocks[thread_id].data(), zsize);
      blocks_written += 1;

      // tout << "final blocks written " << blocks_written << " thread " << thread_id << "\n" << std::flush;

    }
  }
  
  void finish() {
    done = true;
    for(unsigned int i =0; i < nthreads; i++) {

      // tout << "joining " << i << "\n" << std::flush;

      threads[i].join();

      // tout << "joined " << i << "\n" << std::flush;

    }
  }
  
  Compress_Thread_Context(std::ofstream* mf, unsigned int nt, QsMetadata qm) : 
    myFile(mf), blocks_total(0), blocks_written(0),
    nthreads(nt-1), compress_level(qm.compress_level), done(false),
    zblocks(std::vector< std::vector<char> >(nthreads, std::vector<char>(this->cenv.compressBound(BLOCKSIZE)))),
    data_blocks(std::vector< std::vector<char> >(nthreads, std::vector<char>(BLOCKSIZE))),
    block_pointers(std::vector< std::pair<const char*, uint64_t> >(nthreads)) {
    
    data_ready = std::vector< std::atomic<bool> >(nthreads);
    for(unsigned int i=0; i<nthreads; i++) {
      data_ready[i] = false;
    }
    
    for (unsigned int i = 0; i < nthreads; i++) {
      threads.push_back(std::thread(&Compress_Thread_Context::worker_thread, this, i));
    }
  }
  
  char* get_new_block_ptr() {

    // tout << "new block\n" << std::flush;

    uint64_t block_check = blocks_total % nthreads;
    while (data_ready[block_check]) {
      std::this_thread::yield();
    }
    block_pointers[block_check].first = data_blocks[block_check].data();
    return data_blocks[block_check].data();
  }
  
  void push_block(const uint32_t datasize) {
    uint64_t block_check = blocks_total % nthreads;

    // tout << "push block " << block_check << "\n" << std::flush;

    block_pointers[block_check].second = datasize;
    data_ready[block_check] = true;
    blocks_total++;
  }
  
  void push_ptr(const char * const ptr, const uint32_t datasize) {
    uint64_t block_check = blocks_total % nthreads;
    
    // tout << "push ptr " << block_check << "\n" << std::flush;

    while (data_ready[block_check]) {
      std::this_thread::yield();
    }
    block_pointers[block_check].first = ptr;
    block_pointers[block_check].second = datasize;
    data_ready[block_check] = true;
    blocks_total++;
  }
};

template <class compress_env> 
struct CompressBuffer_MT {
  QsMetadata qm;
  std::ofstream * myFile;
  xxhash_env xenv;
  Compress_Thread_Context<compress_env> ctc;
  CountToObjectMap object_ref_hash;
  
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  // shuffle_endblock is tracking when shuffleblock is finished processing
  uint64_t shuffle_endblock = 0;
  
  uint64_t current_blocksize = 0;
  uint64_t number_of_blocks = 0;
  char* block_data_ptr;
  
  CompressBuffer_MT(std::ofstream * f, QsMetadata _qm, unsigned int nthreads) : qm(_qm), myFile(f), ctc(f, nthreads, _qm) {
    block_data_ptr = ctc.get_new_block_ptr();
  }
  void flush() {
    if(current_blocksize > 0) {
      ctc.push_block(current_blocksize);
      number_of_blocks++;
      current_blocksize = 0;
      block_data_ptr = ctc.get_new_block_ptr();
    }
  }
  void push_contiguous(const char * const data, const uint64_t len) {
    if(qm.check_hash) xenv.update(data, len);
    uint64_t current_pointer_consumed = 0;
    while(current_pointer_consumed < len) {
      if( current_blocksize == BLOCKSIZE ) {
        flush();
      }
      if(current_blocksize == 0 && len - current_pointer_consumed >= BLOCKSIZE) {
        ctc.push_ptr(data + current_pointer_consumed, BLOCKSIZE);
        current_pointer_consumed += BLOCKSIZE;
        block_data_ptr = ctc.get_new_block_ptr();
        number_of_blocks++;
      } else {
        uint64_t remaining_pointer_available = len - current_pointer_consumed;
        uint64_t add_length = remaining_pointer_available < (BLOCKSIZE - current_blocksize) ? remaining_pointer_available : BLOCKSIZE-current_blocksize;
        std::memcpy(block_data_ptr + current_blocksize, data + current_pointer_consumed, add_length);
        current_blocksize += add_length;
        current_pointer_consumed += add_length;
      }
    }
  }
  void push_noncontiguous(const char * const data, const uint64_t len) {
    if(qm.check_hash) xenv.update(data, len);
    uint64_t current_pointer_consumed = 0;
    while(current_pointer_consumed < len) {
      if( BLOCKSIZE - current_blocksize < BLOCKRESERVE ) {
        flush();
      }
      if(current_blocksize == 0 && len - current_pointer_consumed >= BLOCKSIZE) {
        ctc.push_ptr(data + current_pointer_consumed, BLOCKSIZE);
        current_pointer_consumed += BLOCKSIZE;
        block_data_ptr = ctc.get_new_block_ptr();
        number_of_blocks++;
      } else {
        uint64_t remaining_pointer_available = len - current_pointer_consumed;
        uint64_t add_length = remaining_pointer_available < (BLOCKSIZE - current_blocksize) ? remaining_pointer_available : BLOCKSIZE-current_blocksize;
        std::memcpy(block_data_ptr + current_blocksize, data + current_pointer_consumed, add_length);
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
  template<typename POD>
  inline void push_pod_noncontiguous(const POD pod1, const POD pod2) {
    std::array<char, sizeof(POD)*2> pdata = {};
    std::memcpy(pdata.data(), reinterpret_cast<const char*>(&pod1), sizeof(POD));
    std::memcpy(pdata.data() + sizeof(POD), reinterpret_cast<const char*>(&pod2), sizeof(POD));
    push_noncontiguous(pdata.data(), sizeof(POD)*2);
  }
  void shuffle_push(const char * const data, const uint64_t len, const uint64_t bytesoftype) {
    if(len > MIN_SHUFFLE_ELEMENTS) {
      // blocks_written = number of blocks file written
      // (len + current_blocksize)/BLOCKSIZE = additional full blocks due to shuffleblock
      // number_of_blocks = number of blocks pushed to ctc
      while( shuffle_endblock > ctc.blocks_written ) {
        
        // tout << "shuffle " << shuffle_endblock << " " << ctc.blocks_written << "\n" << std::flush;
        
        std::this_thread::yield();
      }
      shuffle_endblock = (len + current_blocksize)/BLOCKSIZE + number_of_blocks;
      if(len > shuffleblock.size()) shuffleblock.resize(len);
      blosc_shuffle(reinterpret_cast<const uint8_t * const>(data), shuffleblock.data(), len, bytesoftype);
      push_contiguous(reinterpret_cast<char*>(shuffleblock.data()), len);
    } else if(len > 0) {
      push_contiguous(data, len);
    }
  }
};
