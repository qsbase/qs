#include "qs_common.h"

struct ZSTD_streamRead {
  std::ifstream & myFile;
  QsMetadata qm;
  uint64_t minblocksize;
  uint64_t maxblocksize;
  uint64_t decompressed_bytes_total;
  uint64_t decompressed_bytes_read;
  std::vector<char> outblock;
  std::vector<char> inblock;
  ZSTD_inBuffer zin;
  ZSTD_outBuffer zout;
  ZSTD_DStream* zds;
  ZSTD_streamRead(std::ifstream & mf, QsMetadata qm, uint64_t dbt) : 
    myFile(mf), qm(qm), decompressed_bytes_total(dbt) {
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
  }
  ~ZSTD_streamRead() {
    ZSTD_freeDStream(zds);
  }
  inline void ZSTD_decompressStream_count(ZSTD_DStream* zds, ZSTD_outBuffer * zout, ZSTD_inBuffer * zin) {
    uint64_t temp = zout->pos;
    size_t return_value = ZSTD_decompressStream(zds, zout, zin);
    if(ZSTD_isError(return_value)) throw std::runtime_error("zstd stream decompression error");
    decompressed_bytes_read += zout->pos - temp;
  }
  void getBlock(uint64_t & blocksize, uint64_t & bytesused) {
    if(decompressed_bytes_read >= decompressed_bytes_total) return;
    char * ptr = outblock.data();
    if(blocksize > bytesused) {
      // dst should never overlap since blocksize > minblocksize
      std::memcpy(ptr, ptr + bytesused, blocksize - bytesused); 
      zout.pos = blocksize - bytesused;
    } else {
      zout.pos = 0;
    }
    while(zout.pos < minblocksize) {
      if(zin.pos < zin.size) {
        ZSTD_decompressStream_count(zds, &zout, &zin);
      } else if(! myFile.eof()) {
        myFile.read(inblock.data(), inblock.size());
        size_t bytes_read = myFile.gcount();
        if(bytes_read == 0) continue; // EOF
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
    bytesused = 0;
  }
  void copyData(uint64_t & blocksize, uint64_t & bytesused,
                char* dst,uint64_t dst_size) {
    char * ptr = outblock.data();
    // dst should never overlap since blocksize > MINBLOCKSIZE
    if(dst_size > blocksize - bytesused) {
      // std::cout << blocksize - bytesused << " data cpy\n";
      std::memcpy(dst, ptr + bytesused, blocksize - bytesused);
      zout.pos = blocksize - bytesused;
      zout.dst = dst;
      zout.size = dst_size;
      while(zout.pos < dst_size) {
        // std::cout << zout.pos << " " << dst_size << " zout position\n";
        if(zin.pos < zin.size) {
          ZSTD_decompressStream_count(zds, &zout, &zin);
          // std::cout << zin.pos << "/" << zin.size << " zin " << zout.pos << "/" << zout.size << " zout\n";
        } else if(! myFile.eof()) {
          myFile.read(inblock.data(), minblocksize);
          size_t bytes_read = myFile.gcount();
          if(bytes_read == 0) continue; // EOF
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
      bytesused = 0;
      blocksize = 0;
    } else {
      std::memcpy(dst, ptr + bytesused, dst_size);
      bytesused += dst_size;
    }
    zout.dst = outblock.data();
    zout.size = maxblocksize;
    if(blocksize - bytesused < BLOCKRESERVE) {
      getBlock(blocksize, bytesused);
    }
  }
};

template <class DestreamClass> 
struct Data_Context_Stream {
  DestreamClass & dsc;
  QsMetadata qm;
  bool use_alt_rep_bool;
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  uint64_t data_offset;
  uint64_t block_size;
  char * data_ptr;
  std::string temp_string;
  
  Data_Context_Stream(DestreamClass & d, QsMetadata q, bool use_alt_rep) : dsc(d), qm(q), use_alt_rep_bool(use_alt_rep) {
    data_offset = 0;
    block_size = 0;
    data_ptr = dsc.outblock.data();
    temp_string = std::string(256, '\0');
  }
  void readHeader(SEXPTYPE & object_type, uint64_t & r_array_len) {
    if(data_offset + BLOCKRESERVE >= block_size) dsc.getBlock(block_size, data_offset);
    char* header = data_ptr;
    readHeader_common(object_type, r_array_len, data_offset, header);
  }
  void readStringHeader(uint32_t & r_string_len, cetype_t & ce_enc) {
    if(data_offset + BLOCKRESERVE >= block_size) dsc.getBlock(block_size, data_offset);
    char* header = data_ptr;
    readStringHeader_common(r_string_len, ce_enc, data_offset, header);
  }
  void getBlockData(char* outp, uint64_t data_size) {
    // std::cout << data_size << " get block\n";
    dsc.copyData(block_size, data_offset, outp, data_size);
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
      }
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
