/* qs - Quick Serialization of R Objects
 Copyright (C) 2019-prsent Travers Ching
 
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

struct Ordered_Counter {
  uint64_t cached_counter_val;
  std::atomic<uint64_t> counter;
  std::vector< std::atomic<uint64_t> > thread_counters;
  Ordered_Counter(unsigned int nthreads) : cached_counter_val(0), counter(0), thread_counters(nthreads) {
    for(unsigned int i=0; i<nthreads; i++) {
      thread_counters[i] = 0;
    }
  }
  void update_counter(unsigned int thread_id) {
    thread_counters[thread_id]++;
  }
  bool check_lte(uint64_t val) {
    if(cached_counter_val > val) return false;
    auto it = std::min_element(thread_counters.begin(), thread_counters.end());
    uint64_t min_val = *it;
    uint64_t first_min = it - thread_counters.begin();
    cached_counter_val = min_val * thread_counters.size() + first_min;
    return cached_counter_val <= val;
  }
  bool check_lt(uint64_t val) {
    if(cached_counter_val >= val) return false;
    auto it = std::min_element(thread_counters.begin(), thread_counters.end());
    uint64_t min_val = *it;
    uint64_t first_min = it - thread_counters.begin();
    cached_counter_val = min_val * thread_counters.size() + first_min;
    return cached_counter_val < val;
  }
};

struct Data_Thread_Context {
  std::ifstream* myFile;
  const unsigned int nthreads;
  uint64_t blocks_total;
  std::atomic<uint64_t> blocks_read;
  std::atomic<uint64_t>  blocks_queued;
  Ordered_Counter blocks_processed;
  
  decompress_fun decompFun;
  cbound_fun cbFun;
  std::vector<std::thread> threads;
  
  std::vector< std::atomic<bool> > primary_block;
  std::vector< std::atomic<bool> > data_signal;
  std::vector< std::atomic<bool> > data_ready; // for direct decompress, set when signal received; for data blocks, set when uncompress finished
  std::vector< std::atomic<bool> > thread_ready;
  
  std::vector< std::vector<char> > zblocks; // one per thread
  std::vector< std::vector<char> > data_blocks; // one per thread
  std::vector< std::vector<char> > data_blocks2; // one per thread
  std::vector< std::atomic<char*> > block_pointers;
  std::vector< std::atomic<uint64_t> > block_sizes;
  
  void worker_thread(unsigned int thread_id) {
    std::array<char,4> zsize_ar;
    for(uint64_t i=thread_id; i < blocks_total; i += nthreads) {
      thread_ready[thread_id] = true;
      //tout << thread_id << " " << i <<  "begin\n" << std::flush;
      while(blocks_read != i) {
        std::this_thread::yield();
      }
      myFile->read(zsize_ar.data(), 4);
      uint32_t zsize = unaligned_cast<uint32_t>(zsize_ar.data(),0);
      myFile->read(zblocks[thread_id].data(), zsize);
      blocks_read++;
      
      //tout << thread_id << " " << zsize << " read done\n" << std::flush;
      
      if(data_signal[thread_id]) {
        thread_ready[thread_id] = false;
        //tout << thread_id << "data ptr set \n" << std::flush;
        if(block_pointers[thread_id] == nullptr) {
          //tout << thread_id << "decompressing block \n" << std::flush;
          if(primary_block[thread_id]) {
            block_sizes[thread_id] = decompFun(data_blocks[thread_id].data(), BLOCKSIZE, zblocks[thread_id].data(), zsize);
            block_pointers[thread_id] = data_blocks[thread_id].data();
          } else {
            block_sizes[thread_id] = decompFun(data_blocks2[thread_id].data(), BLOCKSIZE, zblocks[thread_id].data(), zsize);
            block_pointers[thread_id] = data_blocks2[thread_id].data();
          }
          data_ready[thread_id] = true;
          //tout << thread_id << " "  << blocks_queued << " done decompressing\n" << std::flush;
        } else {
          data_ready[thread_id] = true;
          //tout << thread_id << "decompressing direct \n" << std::flush;
          decompFun(block_pointers[thread_id], BLOCKSIZE, zblocks[thread_id].data(), zsize); // if decompressing directly, we don't need to wait
          //tout << thread_id << "done decompressing direct\n" << std::flush;
        }
      } else {
        //tout << thread_id << "decompressing block ahead of signal \n" << std::flush;
        if(primary_block[thread_id]) {
          block_sizes[thread_id] = decompFun(data_blocks[thread_id].data(), BLOCKSIZE, zblocks[thread_id].data(), zsize);
          
        } else {
          block_sizes[thread_id] = decompFun(data_blocks2[thread_id].data(), BLOCKSIZE, zblocks[thread_id].data(), zsize);
          data_blocks2[thread_id].data();
        }
        while(!data_signal[thread_id]) std::this_thread::yield();
        thread_ready[thread_id] = false;
        if(block_pointers[thread_id] == nullptr) {
          if(primary_block[thread_id]) {
            block_pointers[thread_id] = data_blocks[thread_id].data();
          } else {
            block_pointers[thread_id] = data_blocks2[thread_id].data();
          }
          data_ready[thread_id] = true;
        } else {
          data_ready[thread_id] = true;
          if(primary_block[thread_id]) {
            std::memcpy(block_pointers[thread_id], data_blocks[thread_id].data(), BLOCKSIZE);
          } else {
            std::memcpy(block_pointers[thread_id], data_blocks2[thread_id].data(), BLOCKSIZE);
          }
        }
      }
      while(data_signal[thread_id]) std::this_thread::yield();
      blocks_processed.update_counter(thread_id);
      primary_block[thread_id] = !primary_block[thread_id];
    }
    //tout << thread_id << " finished thread for loop\n" << std::flush;
  }
  
  void finish() {
    blocks_queued++;
    for(unsigned int i=0; i < nthreads; i++) {
      //tout << "join called " << i << "\n" << std::flush;
      threads[i].join();
    }
  }
  
  Data_Thread_Context(std::ifstream* mf, unsigned int nt, QsMetadata qm) : 
    myFile(mf), nthreads(nt), blocks_read(0), blocks_queued(0), blocks_processed(nt) {
    blocks_total = readSizeFromFile8(*myFile);
    //tout << blocks_total << " total blocks\n" << std::flush;
    if(qm.compress_algorithm == 0) {
      decompFun = &ZSTD_decompress;
      cbFun = &ZSTD_compressBound;
    } else { // algo == 1
      decompFun = &LZ4_decompress_fun;
      cbFun = &LZ4_compressBound_fun;
    }
    
    zblocks = std::vector<std::vector<char> >(nthreads, std::vector<char>(cbFun(BLOCKSIZE)));
    data_blocks = std::vector<std::vector<char> >(nthreads, std::vector<char>(BLOCKSIZE));
    data_blocks2 = std::vector<std::vector<char> >(nthreads, std::vector<char>(BLOCKSIZE));
    block_pointers = std::vector<std::atomic<char*>>(nthreads);
    for(unsigned int i=0; i<nthreads; i++) {
      block_pointers[i] = nullptr;
    }
    block_sizes = std::vector<std::atomic<uint64_t>>(nthreads);
    for(unsigned int i=0; i<nthreads; i++) {
      block_sizes[i] = 0;
    }
    primary_block = std::vector< std::atomic<bool> >(nthreads);
    for(unsigned int i=0; i<nthreads; i++) {
      primary_block[i] = true;
    }
    data_ready = std::vector< std::atomic<bool> >(nthreads);
    for(unsigned int i=0; i<nthreads; i++) {
      data_ready[i] = false;
    }
    data_signal = std::vector< std::atomic<bool> >(nthreads);
    for(unsigned int i=0; i<nthreads; i++) {
      data_signal[i] = false;
    }
    thread_ready = std::vector< std::atomic<bool> >(nthreads);
    for(unsigned int i=0; i<nthreads; i++) {
      thread_ready[i] = false;
    }
    for (unsigned int i = 0; i < nthreads; i++) {
      threads.push_back(std::thread(&Data_Thread_Context::worker_thread, this, i));
    }
  }
  
  std::pair<char*, uint64_t> get_block_ptr() {
    uint64_t current_block = blocks_queued % nthreads;
    blocks_queued++;
    while(!thread_ready[current_block]) std::this_thread::yield();
    block_pointers[current_block] = nullptr;
    data_signal[current_block] = true;
    while(!data_ready[current_block]) std::this_thread::yield();
    char* temp_ptr = block_pointers[current_block];
    uint64_t temp_size = block_sizes[current_block];
    //tout << "getting block ptr " << blocks_queued << " " << temp_size << " " << (void *)(temp_ptr) << "\n" << std::flush;
    data_ready[current_block] = false;
    data_signal[current_block] = false;
    return std::pair<char*, uint64_t>(temp_ptr, temp_size);
  }
  
  void decompress_data_direct(char* bpointer) {
    uint64_t current_block = blocks_queued % nthreads;
    //tout << "decomp direct call " << blocks_queued << "\n" << std::flush;
    blocks_queued++;
    while(!thread_ready[current_block]) std::this_thread::yield();
    block_pointers[current_block] = bpointer;
    data_signal[current_block] = true;
    while(!data_ready[current_block]) std::this_thread::yield();
    data_ready[current_block] = false;
    data_signal[current_block] = false;
  }
};


struct Data_Context_MT {
  std::ifstream * myFile;
  bool use_alt_rep_bool;
  Data_Thread_Context dtc;
  QsMetadata qm;
  
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  uint64_t data_offset;
  char* block_data = nullptr;
  uint64_t block_size;
  std::string temp_string;
  
  Data_Context_MT(std::ifstream * mf, QsMetadata qm, bool use_alt_rep, unsigned int nthreads) : 
    myFile(mf), use_alt_rep_bool(use_alt_rep), dtc(mf, nthreads-1, qm) {
    this->qm = qm;
    data_offset = 0;
    block_size = 0;
    temp_string = std::string(256, '\0');
  }
  void readHeader(SEXPTYPE & object_type, uint64_t & r_array_len) {
    if(data_offset >= block_size) decompress_block();
    char* header = block_data;
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
    char* header = block_data;
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
    dtc.decompress_data_direct(bpointer);
  }
  void decompress_block() {
    auto res = dtc.get_block_ptr();
    block_data = res.first;
    block_size = res.second;
    data_offset = 0;
    //tout << "main thread decompress block " << (void *)block_data << " " << block_size << "\n" << std::flush;
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
      uint64_t shuffle_endblock = (data_size + data_offset)/BLOCKSIZE + dtc.blocks_queued;
      dtc.blocks_processed.check_lt(shuffle_endblock);
      //tout << "shuffle end is " << shuffle_endblock << " " << dtc.blocks_processed.cached_counter_val << "\n" << std::flush;
      getBlockData(reinterpret_cast<char*>(shuffleblock.data()), data_size);
      while( dtc.blocks_processed.check_lt(shuffle_endblock) ) {
        std::this_thread::yield();
      }
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
    switch(obj_type) {
    case VECSXP: 
      obj = PROTECT(Rf_allocVector(VECSXP, r_array_len));
      for(uint64_t i=0; i<r_array_len; i++) {
        SET_VECTOR_ELT(obj, i, processBlock());
      }
      break;
    case REALSXP:
      obj = PROTECT(Rf_allocVector(REALSXP, r_array_len));
      if(qm.real_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(REAL(obj)), r_array_len*8, 8);
      } else {
        getBlockData(reinterpret_cast<char*>(REAL(obj)), r_array_len*8);
      }
      break;
    case INTSXP:
      obj = PROTECT(Rf_allocVector(INTSXP, r_array_len));
      if(qm.int_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(INTEGER(obj)), r_array_len*4, 4);
      } else {
        getBlockData(reinterpret_cast<char*>(INTEGER(obj)), r_array_len*4);
      }
      break;
    case LGLSXP:
      obj = PROTECT(Rf_allocVector(LGLSXP, r_array_len));
      if(qm.lgl_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(LOGICAL(obj)), r_array_len*4, 4);
      } else {
        getBlockData(reinterpret_cast<char*>(LOGICAL(obj)), r_array_len*4);
      }
      break;
    case CPLXSXP:
      obj = PROTECT(Rf_allocVector(CPLXSXP, r_array_len));
      if(qm.cplx_shuffle) {
        getShuffleBlockData(reinterpret_cast<char*>(COMPLEX(obj)), r_array_len*16, 8);
      } else {
        getBlockData(reinterpret_cast<char*>(COMPLEX(obj)), r_array_len*16);
      }
      break;
    case RAWSXP:
      obj = PROTECT(Rf_allocVector(RAWSXP, r_array_len));
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
            getBlockData(&(ret->strings[i])[0], r_string_len); // don't need to wait!
          }
        }
        obj = PROTECT(stdvec_string::Make(ret, true));
      } else {
        obj = PROTECT(Rf_allocVector(STRSXP, r_array_len));
        uint64_t string_endblock;
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
            if(data_offset + r_string_len > BLOCKSIZE) {
              string_endblock = (r_string_len + data_offset)/BLOCKSIZE + dtc.blocks_queued;
              getBlockData(&temp_string[0], r_string_len);
              while( dtc.blocks_processed.check_lt(string_endblock) ) {
                std::this_thread::yield();
              }
            } else {
              getBlockData(&temp_string[0], r_string_len);
            }
            SET_STRING_ELT(obj, i, Rf_mkCharLenCE(temp_string.data(), r_string_len, string_encoding));
          }
        }
      }
      break;
    case S4SXP:
    {
      SEXP obj_data = PROTECT(Rf_allocVector(RAWSXP, r_array_len));
      getBlockData(reinterpret_cast<char*>(RAW(obj_data)), r_array_len);
      obj = PROTECT(unserializeFromRaw(obj_data));
      UNPROTECT(2);
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
        uint64_t string_endblock;
        readStringHeader(r_string_len, string_encoding);
        std::string temp_attribute_string = std::string(r_string_len, '\0');
        if(data_offset + r_string_len > BLOCKSIZE) {
          string_endblock = (r_string_len + data_offset)/BLOCKSIZE + dtc.blocks_queued;
          getBlockData(&temp_attribute_string[0], r_string_len);
          while( dtc.blocks_processed.check_lt(string_endblock) ) {
            std::this_thread::yield();
          }
        } else {
          getBlockData(&temp_attribute_string[0], r_string_len);
        }
        Rf_setAttrib(obj, Rf_install(temp_attribute_string.data()), processBlock());
      }
    }
    UNPROTECT(1);
    return std::move(obj);
  }
};