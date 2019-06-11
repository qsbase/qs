#include "qs_common.h"

struct ZSTD_streamWrite {
  std::ofstream & myFile;
  QsMetadata qm;
  uint64_t bytes_written;
  std::vector<char> outblock;
  ZSTD_inBuffer zin;
  ZSTD_outBuffer zout;
  ZSTD_CStream* zcs;
  ZSTD_streamWrite(std::ofstream & mf, QsMetadata qm) : myFile(mf), qm(qm) {
    bytes_written = 0;
    size_t outblocksize = ZSTD_CStreamOutSize();
    outblock = std::vector<char>(outblocksize);
    zcs = ZSTD_createCStream();
    ZSTD_initCStream(zcs, qm.compress_level);
    zout.size = outblocksize;
    zout.pos = 0;
    zout.dst = outblock.data();
  }
  ~ZSTD_streamWrite() {
    ZSTD_freeCStream(zcs);
  }
  void push(char * data, uint64_t length) {
    zin.pos = 0;
    zin.src = data;
    zin.size = length;
    bytes_written += zin.size;
    while(zin.pos < zin.size) {
      zout.pos = 0;
      size_t return_value = ZSTD_compressStream(zcs, &zout, &zin);
      if(ZSTD_isError(return_value)) throw exception("zstd stream compression error; output is likely corrupted");
      if(zout.pos > 0) myFile.write(reinterpret_cast<char*>(zout.dst), zout.pos);
    }
  }
  void flush() {
    size_t remain;
    do {
      zout.pos = 0;
      remain = ZSTD_flushStream(zcs, &zout);
      if(ZSTD_isError(remain)) throw exception("zstd stream compression error; output is likely corrupted");
      if(zout.pos > 0) myFile.write(reinterpret_cast<char*>(zout.dst), zout.pos);
    } while (remain != 0);
  }
};

// struct brotli_streamWrite {
//   std::ofstream & myFile;
//   QsMetadata qm;
//   uint64_t bytes_written;
//   std::vector<char> outblock;
//   BrotliEncoderState* zcs;
//   brotli_streamWrite(std::ofstream & mf, QsMetadata qm) : myFile(mf), qm(qm) {
//     bytes_written = 0;
//     outblock = std::vector<char>(BLOCKSIZE);
//     zcs = BrotliEncoderCreateInstance(NULL, NULL, NULL);
//     BrotliEncoderSetParameter(zcs, BROTLI_PARAM_QUALITY, static_cast<uint32_t>(qm.compress_level));
//     BrotliEncoderSetParameter(zcs, BROTLI_PARAM_LGWIN, 30);
//   }
//   ~brotli_streamWrite() {
//     BrotliEncoderDestroyInstance(zcs);
//   }
//   void push(char * data, size_t length) {
//     const uint8_t * next_in = reinterpret_cast<uint8_t*>(data);
//     bytes_written += length;
//     uint8_t * next_out = reinterpret_cast<uint8_t*>(outblock.data());
//     size_t available_out = outblock.size();
//     while(length > 0) {
//       BrotliEncoderCompressStream(zcs, BROTLI_OPERATION_PROCESS, 
//                                   &length, &next_in, 
//                                   &available_out, &next_out, NULL);
//       if(available_out == 0) {
//         myFile.write(outblock.data(), outblock.size());
//         available_out = outblock.size();
//         next_out = reinterpret_cast<uint8_t*>(outblock.data());
//       }
//     }
//     if(available_out < outblock.size()) {
//       myFile.write(outblock.data(), outblock.size() - available_out);
//     }
//   }
//   void flush() {
//     // while(BrotliEncoderHasMoreOutput(zcs)) {
//     //   size_t temp = 0;
//     //   uint8_t * next_out = reinterpret_cast<uint8_t*>(outblock.data());
//     //   size_t available_out = outblock.size();
//     //   BrotliEncoderCompressStream(zcs, BROTLI_OPERATION_FLUSH, &temp, 
//     //                               NULL, &available_out, reinterpret_cast<uint8_t**>(&next_out), NULL);
//     //   if(available_out != outblock.size()) {
//     //     myFile.write(outblock.data(), outblock.size());
//     //     available_out = outblock.size();
//     //     next_out = reinterpret_cast<uint8_t*>(outblock.data());
//     //   }
//     // }
//     while(! BrotliEncoderIsFinished(zcs)) {
//       size_t temp = 0;
//       uint8_t * next_out = reinterpret_cast<uint8_t*>(outblock.data());
//       size_t available_out = outblock.size();
//       BrotliEncoderCompressStream(zcs, BROTLI_OPERATION_FINISH, &temp, 
//                                   NULL, &available_out, reinterpret_cast<uint8_t**>(&next_out), NULL);
//       if(available_out != outblock.size()) {
//         myFile.write(outblock.data(), outblock.size() - available_out);
//         available_out = outblock.size();
//         next_out = reinterpret_cast<uint8_t*>(outblock.data());
//       }
//     }
//   }
// };

template <class StreamClass> 
struct CompressBufferStream {
  StreamClass & sobj;
  std::vector<uint8_t> shuffleblock = std::vector<uint8_t>(256);
  std::vector<char> block = std::vector<char>(BLOCKSIZE);
  QsMetadata qm;

  CompressBufferStream(StreamClass & so, QsMetadata qm) : sobj(so), qm(qm) {}
  void shuffle_push(char* data, uint64_t len, size_t bytesoftype) {
    if(len > MIN_SHUFFLE_ELEMENTS) {
      if(len > shuffleblock.size()) shuffleblock.resize(len);
      blosc_shuffle(reinterpret_cast<uint8_t*>(data), shuffleblock.data(), len, bytesoftype);
      sobj.push(reinterpret_cast<char*>(shuffleblock.data()), len);
    } else if(len > 0) {
      sobj.push(data, len);
    }
  }
  template<typename POD>
  inline void push_pod(POD pod) {
    sobj.push(reinterpret_cast<char*>(&pod), sizeof(pod));
  }
  
  void writeHeader(SEXPTYPE object_type, uint64_t length) {
    switch(object_type) {
    case REALSXP:
      if(length < 32) {
        push_pod(static_cast<unsigned char>( numeric_header_5 | static_cast<unsigned char>(length) ) );
      } else if(length < 256) { 
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_8)), 1);
        push_pod(static_cast<uint8_t>(length) );
      } else if(length < 65536) { 
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_16)), 1);
        push_pod(static_cast<uint16_t>(length) );
      } else if(length < 4294967296) {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_32)), 1);
        push_pod(static_cast<uint32_t>(length) );
      } else {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&numeric_header_64)), 1);
        push_pod(static_cast<uint64_t>(length) );
      }
      return;
    case VECSXP:
      if(length < 32) {
        push_pod(static_cast<unsigned char>( list_header_5 | static_cast<unsigned char>(length) ) );
      } else if(length < 256) { 
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_8)), 1);
        push_pod(static_cast<uint8_t>(length) );
      } else if(length < 65536) { 
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_16)), 1);
        push_pod(static_cast<uint16_t>(length) );
      } else if(length < 4294967296) {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_32)), 1);
        push_pod(static_cast<uint32_t>(length) );
      } else {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&list_header_64)), 1);
        push_pod(static_cast<uint64_t>(length) );
      }
      return;
    case INTSXP:
      if(length < 32) {
        push_pod(static_cast<unsigned char>( integer_header_5 | static_cast<unsigned char>(length) ) );
      } else if(length < 256) { 
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_8)), 1);
        push_pod(static_cast<uint8_t>(length) );
      } else if(length < 65536) { 
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_16)), 1);
        push_pod(static_cast<uint16_t>(length) );
      } else if(length < 4294967296) {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_32)), 1);
        push_pod(static_cast<uint32_t>(length) );
      } else {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&integer_header_64)), 1);
        push_pod(static_cast<uint64_t>(length) );
      }
      return;
    case LGLSXP:
      if(length < 32) {
        push_pod(static_cast<unsigned char>( logical_header_5 | static_cast<unsigned char>(length) ) );
      } else if(length < 256) { 
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_8)), 1);
        push_pod(static_cast<uint8_t>(length) );
      } else if(length < 65536) { 
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_16)), 1);
        push_pod(static_cast<uint16_t>(length) );
      } else if(length < 4294967296) {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_32)), 1);
        push_pod(static_cast<uint32_t>(length) );
      } else {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&logical_header_64)), 1);
        push_pod(static_cast<uint64_t>(length) );
      }
      return;
    case RAWSXP:
      if(length < 4294967296) {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&raw_header_32)), 1);
        push_pod(static_cast<uint32_t>(length) );
      } else {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&raw_header_64)), 1);
        push_pod(static_cast<uint64_t>(length) );
      }
      return;
    case STRSXP:
      if(length < 32) {
        push_pod(static_cast<unsigned char>( character_header_5 | static_cast<unsigned char>(length) ) );
      } else if(length < 256) { 
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_8)), 1);
        push_pod(static_cast<uint8_t>(length) );
      } else if(length < 65536) { 
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_16)), 1);
        push_pod(static_cast<uint16_t>(length) );
      } else if(length < 4294967296) {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_32)), 1);
        push_pod(static_cast<uint32_t>(length) );
      } else {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&character_header_64)), 1);
        push_pod(static_cast<uint64_t>(length) );
      }
      return;
    case CPLXSXP:
      if(length < 4294967296) {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&complex_header_32)), 1);
        push_pod(static_cast<uint32_t>(length) );
      } else {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&complex_header_64)), 1);
        push_pod(static_cast<uint64_t>(length) );
      }
      return;
    case NILSXP:
      sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&null_header)), 1);
      return;
    default:
      throw exception("something went wrong writing object header");  // should never reach here
    }
  }
  
  void writeAttributeHeader(uint64_t length) {
    if(length < 32) {
      push_pod(static_cast<unsigned char>( attribute_header_5 | static_cast<unsigned char>(length) ) );
    } else if(length < 256) {
      push_pod(static_cast<unsigned char>( attribute_header_8 ) );
      push_pod(static_cast<uint8_t>(length) );
    } else {
      push_pod(static_cast<unsigned char>( attribute_header_32 ) );
      push_pod(static_cast<uint32_t>(length) );
    }
  }
  
  void writeStringHeader(uint64_t length, cetype_t ce_enc) {
    unsigned char enc;
    switch(ce_enc) {
    case CE_NATIVE:
      enc = string_enc_native; break;
    case CE_UTF8:
      enc = string_enc_utf8; break;
    case CE_LATIN1:
      enc = string_enc_latin1; break;
    case CE_BYTES:
      enc = string_enc_bytes; break;
    default:
      enc = string_enc_native;
    }
    if(length < 32) {
      push_pod(static_cast<unsigned char>( string_header_5 | static_cast<unsigned char>(enc) | static_cast<unsigned char>(length) ) );
    } else if(length < 256) {
      push_pod(static_cast<unsigned char>( string_header_8 | static_cast<unsigned char>(enc) ) );
      push_pod(static_cast<uint8_t>(length) );
    } else if(length < 65536) {
      push_pod(static_cast<unsigned char>( string_header_16 | static_cast<unsigned char>(enc) ) );
      push_pod(static_cast<uint16_t>(length) );
    } else {
      push_pod(static_cast<unsigned char>( string_header_32 | static_cast<unsigned char>(enc) ) );
      push_pod(static_cast<uint32_t>(length) );
    }
  }
  
  // to do: use SEXP instead of RObject?
  void pushObj(RObject & x, bool attributes_processed = false) {
    if(!attributes_processed && stypes.find(TYPEOF(x)) != stypes.end()) {
      std::vector<std::string> anames = x.attributeNames();
      if(anames.size() != 0) {
        writeAttributeHeader(anames.size());
        pushObj(x, true);
        for(uint64_t i=0; i<anames.size(); i++) {
          writeStringHeader(anames[i].size(),CE_NATIVE);
          sobj.push(&anames[i][0], anames[i].size());
          RObject xa = x.attr(anames[i]);
          pushObj(xa);
        }
      } else {
        pushObj(x, true);
      }
    } else if(TYPEOF(x) == STRSXP) {
      uint64_t dl = Rf_xlength(x);
      writeHeader(STRSXP, dl);
      CharacterVector xc = CharacterVector(x);
      for(uint64_t i=0; i<dl; i++) {
        SEXP xi = xc[i];
        if(xi == NA_STRING) {
          sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&string_header_NA)), 1);
        } else {
          uint64_t dl = LENGTH(xi);
          writeStringHeader(dl, Rf_getCharCE(xi));
          sobj.push(const_cast<char*>(CHAR(xi)), dl);
        }
      }
    } else if(stypes.find(TYPEOF(x)) != stypes.end()) {
      uint64_t dl = Rf_xlength(x);
      writeHeader(TYPEOF(x), dl);
      if(TYPEOF(x) == VECSXP) {
        List xl = List(x);
        for(uint64_t i=0; i<dl; i++) {
          RObject xi = xl[i];
          pushObj(xi);
        }
      } else {
        switch(TYPEOF(x)) {
        case REALSXP:
          if(qm.real_shuffle) {
            shuffle_push(reinterpret_cast<char*>(REAL(x)), dl*8, 8);
          } else {
            sobj.push(reinterpret_cast<char*>(REAL(x)), dl*8); 
          }
          break;
        case INTSXP:
          if(qm.int_shuffle) {
            shuffle_push(reinterpret_cast<char*>(INTEGER(x)), dl*4, 4); break;
          } else {
            sobj.push(reinterpret_cast<char*>(INTEGER(x)), dl*4); 
          }
          break;
        case LGLSXP:
          if(qm.lgl_shuffle) {
            shuffle_push(reinterpret_cast<char*>(LOGICAL(x)), dl*4, 4); break;
          } else {
            sobj.push(reinterpret_cast<char*>(LOGICAL(x)), dl*4); 
          }
          break;
        case RAWSXP:
          sobj.push(reinterpret_cast<char*>(RAW(x)), dl); 
          break;
        case CPLXSXP:
          if(qm.cplx_shuffle) {
            shuffle_push(reinterpret_cast<char*>(COMPLEX(x)), dl*16, 8); break;
          } else {
            sobj.push(reinterpret_cast<char*>(COMPLEX(x)), dl*16); 
          }
          break;
        case NILSXP:
          break;
        }
      }
    } else { // other non-supported SEXPTYPEs use the built in R serialization method
      RawVector xserialized = serializeToRaw(x);
      if(xserialized.size() < 4294967296) {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&nstype_header_32)), 1);
        push_pod(static_cast<uint32_t>(xserialized.size()) );
      } else {
        sobj.push(reinterpret_cast<char*>(const_cast<unsigned char*>(&nstype_header_64)), 1);
        push_pod(static_cast<uint64_t>(xserialized.size()) );
      }
      sobj.push(reinterpret_cast<char*>(RAW(xserialized)), xserialized.size());
    }
  }
};
