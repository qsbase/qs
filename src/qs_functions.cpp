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
// #include "qs_inspect.h"
#include "qs_serialization.h"
#include "qs_mt_serialization.h"
#include "qs_deserialization.h"
#include "qs_mt_deserialization.h"
#include "qs_serialization_stream.h"
#include "qs_deserialization_stream.h"
  
/*
 * headers:
 * qs_common.h -> qs_serialization.h -> qs_functions.cpp
 * qs_common.h -> qs_deserialization.h -> qs_functions.cpp
 * qs_common.h -> qs_mt_serialization.h -> qs_functions.cpp
 * qs_common.h -> qs_mt_deserialization.h -> qs_functions.cpp
 * qs_common.h -> qs_serialization_stream.h -> qs_functions.cpp
 * qs_common.h -> qs_deserialization_stream.h -> qs_functions.cpp
 * qs_common.h is protected with an include guard
 */

// https://stackoverflow.com/a/1001373
// [[Rcpp::export]]
bool is_big_endian()
{
  union {
  uint32_t i;
  char c[4];
} bint = {0x01020304};
  return bint.c[0] == 1; 
}

// [[Rcpp::export]]
double c_qsave(SEXP x, std::string file, std::string preset, std::string algorithm, int compress_level, int shuffle_control, bool check_hash, int nthreads) {
  std::ofstream myFile(file.c_str(), std::ios::out | std::ios::binary);
  if(!myFile) {
    throw std::runtime_error("Failed to open file");
  }
  std::streampos origin = myFile.tellp();
  QsMetadata qm(preset, algorithm, compress_level, shuffle_control, check_hash);
  qm.writeToFile(myFile);
  std::streampos header_end_pos = myFile.tellp();
  writeSize8(myFile, 0); // number of compressed blocks
  uint64_t clength;
  if(qm.compress_algorithm == static_cast<unsigned char>(compalg::zstd_stream)) {
    ZSTD_streamWrite<std::ofstream> sw(myFile, qm);
    CompressBufferStream<ZSTD_streamWrite<std::ofstream>> vbuf(sw, qm);
    vbuf.pushObj(x);
    sw.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.sobj.xenv.digest());
    clength = sw.bytes_written;
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::uncompressed)) {
    uncompressed_streamWrite<std::ofstream> sw(myFile, qm);
    CompressBufferStream<uncompressed_streamWrite<std::ofstream>> vbuf(sw, qm);
    vbuf.pushObj(x);
    if(qm.check_hash) writeSize4(myFile, vbuf.sobj.xenv.digest());
    clength = sw.bytes_written;
  } else {
    if(nthreads <= 1) {
      if(qm.compress_algorithm == static_cast<unsigned char>(compalg::zstd)) {
        CompressBuffer<std::ofstream, zstd_compress_env> vbuf(myFile, qm);
        vbuf.pushObj(x);
        vbuf.flush();
        // std::cout << vbuf.xenv.digest() << std::endl;
        if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
        clength = vbuf.number_of_blocks;
      } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::lz4)) {
        CompressBuffer<std::ofstream, lz4_compress_env> vbuf(myFile, qm);
        vbuf.pushObj(x);
        vbuf.flush();
        // std::cout << vbuf.xenv.digest() << std::endl;
        if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
        clength = vbuf.number_of_blocks;
      } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::lz4hc)) {
        CompressBuffer<std::ofstream, lz4hc_compress_env> vbuf(myFile, qm);
        vbuf.pushObj(x);
        vbuf.flush();
        // std::cout << vbuf.xenv.digest() << std::endl;
        if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
        clength = vbuf.number_of_blocks;
      } else {
        throw std::runtime_error("invalid compression algorithm selected");
      }
    } else {
      if(qm.compress_algorithm == static_cast<unsigned char>(compalg::zstd)) {
        CompressBuffer_MT<zstd_compress_env> vbuf(&myFile, qm, nthreads);
        vbuf.pushObj(x);
        vbuf.flush();
        vbuf.ctc.finish();
        if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
        clength = vbuf.number_of_blocks;
      } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::lz4)) {
        CompressBuffer_MT<lz4_compress_env> vbuf(&myFile, qm, nthreads);
        vbuf.pushObj(x);
        vbuf.flush();
        vbuf.ctc.finish();
        if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
        clength = vbuf.number_of_blocks;
      } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::lz4hc)) {
        CompressBuffer_MT<lz4hc_compress_env> vbuf(&myFile, qm, nthreads);
        vbuf.pushObj(x);
        vbuf.flush();
        vbuf.ctc.finish();
        if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
        clength = vbuf.number_of_blocks;
      } else {
        throw std::runtime_error("invalid compression algorithm selected");
      }
    }
  }
  uint64_t total_file_size = myFile.tellp() - origin;
  myFile.seekp(header_end_pos);
  writeSize8(myFile, clength);
  return static_cast<double>(total_file_size);
}

// [[Rcpp::export]]
double c_qsave_fd(SEXP x, int fd, std::string preset, std::string algorithm, int compress_level, int shuffle_control, bool check_hash) {
  fd_wrapper myFile(fd);
  QsMetadata qm(preset, algorithm, compress_level, shuffle_control, check_hash);
  qm.writeToFile(myFile);
  writeSize8(myFile, 0); // number of compressed blocks
  if(qm.compress_algorithm == static_cast<unsigned char>(compalg::zstd_stream)) {
    ZSTD_streamWrite<fd_wrapper> sw(myFile, qm);
    CompressBufferStream<ZSTD_streamWrite<fd_wrapper>> vbuf(sw, qm);
    vbuf.pushObj(x);
    sw.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.sobj.xenv.digest());
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::uncompressed)) {
    uncompressed_streamWrite<fd_wrapper> sw(myFile, qm);
    CompressBufferStream<uncompressed_streamWrite<fd_wrapper>> vbuf(sw, qm);
    vbuf.pushObj(x);
    if(qm.check_hash) writeSize4(myFile, vbuf.sobj.xenv.digest());
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::zstd)) {
    CompressBuffer<fd_wrapper, zstd_compress_env> vbuf(myFile, qm);
    vbuf.pushObj(x);
    vbuf.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::lz4)) {
    CompressBuffer<fd_wrapper, lz4_compress_env> vbuf(myFile, qm);
    vbuf.pushObj(x);
    vbuf.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::lz4hc)) {
    CompressBuffer<fd_wrapper, lz4hc_compress_env> vbuf(myFile, qm);
    vbuf.pushObj(x);
    vbuf.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
  } else {
    throw std::runtime_error("invalid compression algorithm selected");
  }
  myFile.flush();
  return static_cast<double>(myFile.bytes_processed);
}

// [[Rcpp::export]]
double c_qsave_handle(SEXP x, SEXP handle, std::string preset, std::string algorithm, int compress_level, int shuffle_control, bool check_hash) {
#ifdef _WIN32
  HANDLE h = R_ExternalPtrAddr(handle);
  handle_wrapper myFile(h);
  QsMetadata qm(preset, algorithm, compress_level, shuffle_control, check_hash);
  qm.writeToFile(myFile);
  writeSize8(myFile, 0); // number of compressed blocks
  if(qm.compress_algorithm == static_cast<unsigned char>(compalg::zstd_stream)) {
    ZSTD_streamWrite<handle_wrapper> sw(myFile, qm);
    CompressBufferStream<ZSTD_streamWrite<handle_wrapper>> vbuf(sw, qm);
    vbuf.pushObj(x);
    sw.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.sobj.xenv.digest());
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::uncompressed)) {
    uncompressed_streamWrite<handle_wrapper> sw(myFile, qm);
    CompressBufferStream<uncompressed_streamWrite<handle_wrapper>> vbuf(sw, qm);
    vbuf.pushObj(x);
    if(qm.check_hash) writeSize4(myFile, vbuf.sobj.xenv.digest());
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::zstd)) {
    CompressBuffer<handle_wrapper, zstd_compress_env> vbuf(myFile, qm);
    vbuf.pushObj(x);
    vbuf.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::lz4)) {
    CompressBuffer<handle_wrapper, lz4_compress_env> vbuf(myFile, qm);
    vbuf.pushObj(x);
    vbuf.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::lz4hc)) {
    CompressBuffer<handle_wrapper, lz4hc_compress_env> vbuf(myFile, qm);
    vbuf.pushObj(x);
    vbuf.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
  } else {
    throw std::runtime_error("invalid compression algorithm selected");
  }
  return static_cast<double>(myFile.bytes_processed);
#else
  throw std::runtime_error("Windows handle only available on windows");
#endif
}

// [[Rcpp::export]]
RawVector c_qserialize(SEXP x, std::string preset, std::string algorithm, int compress_level, int shuffle_control, bool check_hash) {
  vec_wrapper myFile;
  QsMetadata qm(preset, algorithm, compress_level, shuffle_control, check_hash);
  qm.writeToFile(myFile);
  uint64_t filesize_offset = myFile.bytes_processed;
  writeSize8(myFile, 0); // number of compressed blocks
  uint64_t clength;
  if(qm.compress_algorithm == static_cast<unsigned char>(compalg::zstd_stream)) {
    ZSTD_streamWrite<vec_wrapper> sw(myFile, qm);
    CompressBufferStream<ZSTD_streamWrite<vec_wrapper>> vbuf(sw, qm);
    vbuf.pushObj(x);
    sw.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.sobj.xenv.digest());
    clength = sw.bytes_written;
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::uncompressed)) {
    uncompressed_streamWrite<vec_wrapper> sw(myFile, qm);
    CompressBufferStream<uncompressed_streamWrite<vec_wrapper>> vbuf(sw, qm);
    vbuf.pushObj(x);
    if(qm.check_hash) writeSize4(myFile, vbuf.sobj.xenv.digest());
    clength = sw.bytes_written;
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::zstd)) {
    CompressBuffer<vec_wrapper, zstd_compress_env> vbuf(myFile, qm);
    vbuf.pushObj(x);
    vbuf.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
    clength = vbuf.number_of_blocks;
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::lz4)) {
    CompressBuffer<vec_wrapper, lz4_compress_env> vbuf(myFile, qm);
    vbuf.pushObj(x);
    vbuf.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
    clength = vbuf.number_of_blocks;
  } else if(qm.compress_algorithm == static_cast<unsigned char>(compalg::lz4hc)) {
    CompressBuffer<vec_wrapper, lz4hc_compress_env> vbuf(myFile, qm);
    vbuf.pushObj(x);
    vbuf.flush();
    if(qm.check_hash) writeSize4(myFile, vbuf.xenv.digest());
    clength = vbuf.number_of_blocks;
  } else {
    throw std::runtime_error("invalid compression algorithm selected");
  }
  myFile.writeDirect(reinterpret_cast<char*>(&clength), 8, filesize_offset);
  myFile.shrink();
  return RawVector(myFile.buffer.begin(), myFile.buffer.end());
}

// [[Rcpp::export]]
SEXP c_qread(std::string file, bool use_alt_rep, bool strict, int nthreads) {
  std::ifstream myFile(file, std::ios::in | std::ios::binary);
  if(!myFile) {
    throw std::runtime_error("Failed to open file");
  }
  Protect_Tracker pt = Protect_Tracker();
  QsMetadata qm = QsMetadata::create(myFile);
  if(qm.compress_algorithm == 3) { // zstd_stream
    ZSTD_streamRead<std::ifstream> sr(myFile, qm);
    Data_Context_Stream<ZSTD_streamRead<std::ifstream>> dc(sr, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, *reinterpret_cast<uint32_t*>(dc.dsc.hash_reserve.data()), dc.dsc.xenv.digest(), dc.dsc.decompressed_bytes_read, strict);
    return ret;
  } else if(qm.compress_algorithm == 4) { // uncompressed
    uncompressed_streamRead<std::ifstream> sr(myFile, qm);
    Data_Context_Stream<uncompressed_streamRead<std::ifstream>> dc(sr, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, *reinterpret_cast<uint32_t*>(dc.dsc.hash_reserve.data()), dc.dsc.xenv.digest(), dc.dsc.decompressed_bytes_read, strict);
    return ret;
  } else {
    if(nthreads <= 1 || qm.clength == 0) {
      if(qm.compress_algorithm == 0) {
        Data_Context<std::ifstream, zstd_decompress_env> dc(myFile, qm, use_alt_rep);
        SEXP ret = PROTECT(dc.processBlock()); pt++;
        validate_data(qm, myFile, readSize4(myFile), dc.xenv.digest(), dc.blocks_read, strict);
        return ret;
      } else if(qm.compress_algorithm == 1 || qm.compress_algorithm == 2) {
        Data_Context<std::ifstream, lz4_decompress_env> dc(myFile, qm, use_alt_rep);
        SEXP ret = PROTECT(dc.processBlock()); pt++;
        validate_data(qm, myFile, readSize4(myFile), dc.xenv.digest(), dc.blocks_read, strict);
        return ret;
      } else {
        throw std::runtime_error("Invalid compression algorithm in file");
      }
    } else {
      if(qm.compress_algorithm == 0) {
        Data_Context_MT<zstd_decompress_env> dc(&myFile, qm, use_alt_rep, nthreads);
        SEXP ret = PROTECT(dc.processBlock()); pt++;
        dc.dtc.finish();
        validate_data(qm, myFile, readSize4(myFile), dc.xenv.digest(), 0, strict);
        return ret;
      } else if(qm.compress_algorithm == 1 || qm.compress_algorithm == 2) {
        Data_Context_MT<lz4_decompress_env> dc(&myFile, qm, use_alt_rep, nthreads);
        SEXP ret = PROTECT(dc.processBlock()); pt++;
        dc.dtc.finish();
        validate_data(qm, myFile, readSize4(myFile), dc.xenv.digest(), 0, strict);
        return ret;
      } else {
        throw std::runtime_error("Invalid compression algorithm in file");
      }
    }
  }
}

// [[Rcpp::export]]
SEXP c_qread_fd(int fd, bool use_alt_rep, bool strict) {
  fd_wrapper myFile(fd);
  Protect_Tracker pt = Protect_Tracker();
  QsMetadata qm  = QsMetadata::create(myFile);
  if(qm.compress_algorithm == 3) { // zstd_stream
    ZSTD_streamRead<fd_wrapper> sr(myFile, qm);
    Data_Context_Stream<ZSTD_streamRead<fd_wrapper>> dc(sr, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, *reinterpret_cast<uint32_t*>(dc.dsc.hash_reserve.data()), dc.dsc.xenv.digest(), dc.dsc.decompressed_bytes_read, strict);
    return ret;
  } else if(qm.compress_algorithm == 4) { // uncompressed
    uncompressed_streamRead<fd_wrapper> sr(myFile, qm);
    Data_Context_Stream<uncompressed_streamRead<fd_wrapper>> dc(sr, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, *reinterpret_cast<uint32_t*>(dc.dsc.hash_reserve.data()), dc.dsc.xenv.digest(), dc.dsc.decompressed_bytes_read, strict);
    return ret;
  } else if(qm.compress_algorithm == 0) {
    Data_Context<fd_wrapper, zstd_decompress_env> dc(myFile, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, readSize4(myFile), dc.xenv.digest(), dc.blocks_read, strict);
    return ret;
  } else if(qm.compress_algorithm == 1 || qm.compress_algorithm == 2) {
    Data_Context<fd_wrapper, lz4_decompress_env> dc(myFile, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, readSize4(myFile), dc.xenv.digest(), dc.blocks_read, strict);
    return ret;
  } else {
    throw std::runtime_error("Invalid compression algorithm in file");
  }
}

// [[Rcpp::export]]
SEXP c_qread_handle(SEXP handle, bool use_alt_rep, bool strict) {
#ifdef _WIN32
  HANDLE h = R_ExternalPtrAddr(handle);
  handle_wrapper myFile(h);
  Protect_Tracker pt = Protect_Tracker();
  QsMetadata qm  = QsMetadata::create(myFile);
  if(qm.compress_algorithm == 3) { // zstd_stream
    ZSTD_streamRead<handle_wrapper> sr(myFile, qm);
    Data_Context_Stream<ZSTD_streamRead<handle_wrapper>> dc(sr, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, *reinterpret_cast<uint32_t*>(dc.dsc.hash_reserve.data()), dc.dsc.xenv.digest(), dc.dsc.decompressed_bytes_read, strict);
    return ret;
  } else if(qm.compress_algorithm == 4) { // uncompressed
    uncompressed_streamRead<handle_wrapper> sr(myFile, qm);
    Data_Context_Stream<uncompressed_streamRead<handle_wrapper>> dc(sr, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, *reinterpret_cast<uint32_t*>(dc.dsc.hash_reserve.data()), dc.dsc.xenv.digest(), dc.dsc.decompressed_bytes_read, strict);
    return ret;
  } else if(qm.compress_algorithm == 0) {
    Data_Context<handle_wrapper, zstd_decompress_env> dc(myFile, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, readSize4(myFile), dc.xenv.digest(), dc.blocks_read, strict);
    return ret;
  } else if(qm.compress_algorithm == 1 || qm.compress_algorithm == 2) {
    Data_Context<handle_wrapper, lz4_decompress_env> dc(myFile, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, readSize4(myFile), dc.xenv.digest(), dc.blocks_read, strict);
    return ret;
  } else {
    throw std::runtime_error("Invalid compression algorithm in file");
  }
#else
  throw std::runtime_error("Windows handle only available on windows");
#endif
}

// [[Rcpp::export]]
SEXP c_qread_ptr(SEXP pointer, double length, bool use_alt_rep, bool strict) {
  void * vp = R_ExternalPtrAddr(pointer);
  mem_wrapper myFile(vp, static_cast<uint64_t>(length));
  Protect_Tracker pt = Protect_Tracker();
  QsMetadata qm = QsMetadata::create(myFile);
  if(qm.compress_algorithm == 3) { // zstd_stream
    ZSTD_streamRead<mem_wrapper> sr(myFile, qm);
    Data_Context_Stream<ZSTD_streamRead<mem_wrapper>> dc(sr, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, *reinterpret_cast<uint32_t*>(dc.dsc.hash_reserve.data()), dc.dsc.xenv.digest(), dc.dsc.decompressed_bytes_read, strict);
    return ret;
  } else if(qm.compress_algorithm == 4) { // uncompressed
    uncompressed_streamRead<mem_wrapper> sr(myFile, qm);
    Data_Context_Stream<uncompressed_streamRead<mem_wrapper>> dc(sr, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, *reinterpret_cast<uint32_t*>(dc.dsc.hash_reserve.data()), dc.dsc.xenv.digest(), dc.dsc.decompressed_bytes_read, strict);
    return ret;
  } else if(qm.compress_algorithm == 0) {
    Data_Context<mem_wrapper, zstd_decompress_env> dc(myFile, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, readSize4(myFile), dc.xenv.digest(), dc.blocks_read, strict);
    return ret;
  } else if(qm.compress_algorithm == 1 || qm.compress_algorithm == 2) {
    Data_Context<mem_wrapper, lz4_decompress_env> dc(myFile, qm, use_alt_rep);
    SEXP ret = PROTECT(dc.processBlock()); pt++;
    validate_data(qm, myFile, readSize4(myFile), dc.xenv.digest(), dc.blocks_read, strict);
    return ret;
  } else {
    throw std::runtime_error("Invalid compression algorithm in file");
  }
}

// [[Rcpp::export]]
SEXP c_qdeserialize(RawVector x, bool use_alt_rep, bool strict) {
  void * p = reinterpret_cast<void*>(RAW(x));
  double dlen = static_cast<double>(Rf_xlength(x));
  Protect_Tracker pt = Protect_Tracker();
  SEXP pointer = PROTECT(R_MakeExternalPtr(p, R_NilValue, R_NilValue)); pt++;
  return c_qread_ptr(pointer, dlen, use_alt_rep, strict);
}

// void c_qsave_fd(SEXP x, std::string scon, int shuffle_control, bool check_hash, std::string popen_mode) {
//   std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(scon.c_str(), popen_mode.c_str()), pclose);
//   if (!pipe) {
//     throw std::runtime_error("popen() failed!");
//   }
//   FILE * con = pipe.get();
//   QsMetadata qm("custom", "uncompressed", 0, shuffle_control, check_hash);
//   qm.writeToCon(con);
//   writeSizeToCon8(con, 0); // this is just zero, we can't seek back to beginning of stream
//   fd_streamWrite sw(con, qm);
//   CompressBufferStream<fd_streamWrite> vbuf(sw, qm);
//   vbuf.pushObj(x);
//   if(qm.check_hash) writeSizeToCon4(con, vbuf.sobj.xenv.digest());
// }

// SEXP c_qread_fd(std::string scon, bool use_alt_rep, bool strict, std::string popen_mode) {
//   std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(scon.c_str(), popen_mode.c_str()), pclose);
//   if (!pipe) {
//     throw std::runtime_error("popen() failed!");
//   }
//   FILE * con = pipe.get();
//   Protect_Tracker pt = Protect_Tracker();
//   QsMetadata qm(con);
//   readSizeFromCon8(con); // zero since it's a stream
//   fd_streamRead sr(con, qm);
//   Data_Context_Stream<fd_streamRead> dc(sr, qm, use_alt_rep);
//   SEXP ret = PROTECT(dc.processBlock()); pt++;
//   sr.validate_hash(strict);
//   return ret;
// }

// [[Rcpp::export]]
RObject c_qdump(std::string file) {
  std::ifstream myFile(file, std::ios::in | std::ios::binary);
  if(!myFile) {
    throw std::runtime_error("Failed to open file");
  }
  QsMetadata qm = QsMetadata::create(myFile);
  List outvec;
  dumpMetadata(outvec, qm);
  uint64_t totalsize = qm.clength;
  std::streampos current = myFile.tellg();
  myFile.ignore(std::numeric_limits<std::streamsize>::max());
  uint64_t readable_bytes = myFile.gcount();
  myFile.seekg(current);
  if(qm.compress_algorithm == 3) { // zstd_stream
    if(qm.check_hash) readable_bytes -= 4;
    RawVector input(readable_bytes);
    RawVector output(totalsize);
    char* inp = reinterpret_cast<char*>(RAW(input));
    char* outp = reinterpret_cast<char*>(RAW(output));
    myFile.read(inp, readable_bytes);
    auto zstream = zstd_decompress_stream_simple(outp, totalsize, inp, readable_bytes);
    bool is_error = zstream.decompress();
    uint32_t computed_hash = XXH32(outp, totalsize, XXH_SEED);
    
    // append results
    outvec["readable_bytes"] = std::to_string(readable_bytes);
    outvec["decompressed_size"] = std::to_string(totalsize);
    outvec["computed_hash"] = std::to_string(computed_hash);
    if(qm.check_hash) {
      uint32_t recorded_hash = readSize4(myFile);
      outvec["recorded_hash"] = std::to_string(recorded_hash);
    }
    outvec["compressed_data"] = input;
    outvec["uncompressed_data"] = output;
    if(is_error) {
      outvec["error"] = "decompression_error";
    }
  } else if(qm.compress_algorithm == 4) { // uncompressed
    if(qm.check_hash) readable_bytes -= 4;
    RawVector input(readable_bytes);
    char* inp = reinterpret_cast<char*>(RAW(input));
    myFile.read(inp, readable_bytes);
    uint32_t computed_hash = XXH32(inp, totalsize, XXH_SEED);
    // append results
    outvec["readable_bytes"] = std::to_string(readable_bytes);
    outvec["decompressed_size"] = std::to_string(totalsize);
    outvec["computed_hash"] = std::to_string(computed_hash);
    if(qm.check_hash) {
      uint32_t recorded_hash = readSize4(myFile);
      outvec["recorded_hash"] = std::to_string(recorded_hash);
    }
    outvec["compressed_data"] = input;
  } else if(qm.compress_algorithm == 0 || qm.compress_algorithm == 1 || qm.compress_algorithm == 2) {
    decompress_fun dfun;
    cbound_fun cbfun;
    iserror_fun errfun;
    if(qm.compress_algorithm == 0) {
      dfun = ZSTD_decompress;
      cbfun = ZSTD_compressBound;
      errfun = ZSTD_isError;
    } else {
      dfun = LZ4_decompress_fun;
      cbfun = LZ4_compressBound_fun;
      errfun = LZ4_isError_fun;
    }
    if(qm.check_hash) readable_bytes -= 4;
    std::vector<char> zblock(cbfun(BLOCKSIZE));
    std::vector<char> block(BLOCKSIZE);
    List output = List(totalsize);
    List input = List(totalsize);
    IntegerVector block_sizes(totalsize);
    IntegerVector zblock_sizes(totalsize);
    xxhash_env xenv = xxhash_env();
    for(uint64_t i=0; i<totalsize; i++) {
      uint64_t zsize = readSize4(myFile);
      if(static_cast<uint64_t>(myFile.gcount()) != 4) break;
      myFile.read(zblock.data(), zsize);
      if(static_cast<uint64_t>(myFile.gcount()) != zsize) break;
      uint64_t block_size = dfun(block.data(), BLOCKSIZE, zblock.data(), zsize);
      if(!errfun(block_size)) {
        xenv.update(block.data(), block_size);
        output[i] = RawVector(block.begin(), block.begin() + block_size);
        input[i] = RawVector(zblock.begin(), zblock.begin() + zsize);
        zblock_sizes[i] = zsize;
        block_sizes[i] = block_size;
      }
    }
    // append results
    outvec["readable_bytes"] = std::to_string(readable_bytes);
    outvec["number_of_blocks"] = std::to_string(totalsize);
    outvec["compressed_block_sizes"] = zblock_sizes;
    outvec["decompressed_block_sizes"] = block_sizes;
    outvec["computed_hash"] = std::to_string(xenv.digest());
    if(qm.check_hash) {
      uint32_t recorded_hash = readSize4(myFile);
      outvec["recorded_hash"] = std::to_string(recorded_hash);
    }
    outvec["compressed_data"] = input;
    outvec["uncompressed_data"] = output;
  } else {
    outvec["error"] = "unknown compression";
  }
  return outvec;
}

// void qs_use_alt_rep(bool s) {
//   use_alt_rep_bool = s;
// }

// [[Rcpp::export]]
std::vector<std::string> randomStrings(int N, int string_size = 50) {
  std::string charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::vector<std::string> ret(N);
  std::string str(string_size, '0');
  for(int i=0; i<N; i++) {
    std::vector<int> r = Rcpp::as< std::vector<int> >(Rcpp::sample(charset.size(), string_size, true, R_NilValue, false));
    for(int j=0; j<string_size; j++) str[j] = charset[r[j]];
    ret[i] = str;
  }
  return ret;
}



////////////////////////////////////////////////////////////////
// functions for users to investigate compression algorithms
////////////////////////////////////////////////////////////////

// [[Rcpp::export]]
int zstd_compress_bound(int size) {
  return ZSTD_compressBound(size);
}

// [[Rcpp::export]]
int lz4_compress_bound(int size) {
  return LZ4_compressBound(size);
}

// [[Rcpp::export]]
std::vector<unsigned char> zstd_compress_raw(RawVector x, int compress_level) {
  if(compress_level > 22 || compress_level < -50) throw std::runtime_error("compress_level must be an integer between -50 and 22");
  uint64_t zsize = ZSTD_compressBound(x.size());
  char* xdata = reinterpret_cast<char*>(RAW(x));
  std::vector<unsigned char> ret(zsize);
  char* retdata = reinterpret_cast<char*>(ret.data());
  zsize = ZSTD_compress(retdata, zsize, xdata, x.size(), compress_level);
  ret.resize(zsize);
  return ret;
}

// [[Rcpp::export]]
RawVector zstd_decompress_raw(RawVector x) {
  uint64_t zsize = x.size();
  char* xdata = reinterpret_cast<char*>(RAW(x));
  uint64_t retsize = ZSTD_getFrameContentSize(xdata, x.size());
  RawVector ret(retsize);
  char* retdata = reinterpret_cast<char*>(RAW(ret));
  ZSTD_decompress(retdata, retsize, xdata, zsize);
  return ret;
}

// [[Rcpp::export]]
std::vector<unsigned char> lz4_compress_raw(RawVector x, int compress_level) {
  if(compress_level < 1) throw std::runtime_error("compress_level must be an integer greater than 1");
  uint64_t zsize = LZ4_compressBound(x.size());
  char* xdata = reinterpret_cast<char*>(RAW(x));
  std::vector<unsigned char> ret(zsize);
  char* retdata = reinterpret_cast<char*>(ret.data());
  zsize = LZ4_compress_fast(xdata, retdata, x.size(), zsize, compress_level);
  ret.resize(zsize);
  return ret;
}

// [[Rcpp::export]]
std::vector<unsigned char> lz4_decompress_raw(RawVector x) {
  int zsize = x.size();
  char* xdata = reinterpret_cast<char*>(RAW(x));
  std::vector<unsigned char> ret(x.size()*3/2);
  
  // char* retdata = reinterpret_cast<char*>(ret.data());
  // int decomp = LZ4_decompress_safe(xdata, retdata, zsize, ret.size());
  int decomp = -1;
  while(ret.size() < INT_MAX) {
    char* retdata = reinterpret_cast<char*>(ret.data());
    decomp = LZ4_decompress_safe(xdata, retdata, zsize, ret.size());
    if(decomp < 0) {
      ret.resize(std::min(ret.size() * 2,  static_cast<size_t>(INT_MAX)));
    } else {
      break;
    }
  }
  if(decomp < 0) throw std::runtime_error("lz4 decompression failed");
  ret.resize(decomp);
  return ret;
}

// [[Rcpp::export]]
std::vector<unsigned char> blosc_shuffle_raw(std::vector<uint8_t> x, int bytesofsize) {
#if defined (__AVX2__)
  Rcpp::Rcerr << "AVX2" << std::endl;
#elif defined (__SSE2__)
  Rcpp::Rcerr << "SSE2" << std::endl;
#else
  Rcpp::Rcerr << "no SIMD" << std::endl;
#endif
  if(bytesofsize != 4 && bytesofsize != 8) throw std::runtime_error("bytesofsize must be 4 or 8");
  size_t blocksize = x.size();
  std::vector<uint8_t> xshuf(blocksize);
  blosc_shuffle(x.data(), xshuf.data(), blocksize, bytesofsize);
  size_t remainder = blocksize % bytesofsize;
  size_t vectorizablebytes = blocksize - remainder;
  std::memcpy(xshuf.data() + vectorizablebytes, x.data() + vectorizablebytes, remainder);
  return xshuf;
}

// [[Rcpp::export]]
std::vector<unsigned char> blosc_unshuffle_raw(std::vector<uint8_t> x, int bytesofsize) {
#if defined (__AVX2__)
  Rcpp::Rcerr << "AVX2" << std::endl;
#elif defined (__SSE2__)
  Rcpp::Rcerr << "SSE2" << std::endl;
#else
  Rcpp::Rcerr << "no SIMD" << std::endl;
#endif
  if(bytesofsize != 4 && bytesofsize != 8) throw std::runtime_error("bytesofsize must be 4 or 8");
  size_t blocksize = x.size();
  std::vector<uint8_t> xshuf(blocksize);
  blosc_unshuffle(x.data(), xshuf.data(), blocksize, bytesofsize);
  size_t remainder = blocksize % bytesofsize;
  size_t vectorizablebytes = blocksize - remainder;
  std::memcpy(xshuf.data() + vectorizablebytes, x.data() + vectorizablebytes, remainder);
  return xshuf;
}

// [[Rcpp::export]]
std::string xxhash_raw(std::vector<uint8_t> x) {
  xxhash_env xenv = xxhash_env();
  xenv.update(x.data(), x.size());
  return std::to_string(xenv.digest());
}

// [[Rcpp::export]]
SEXP convertToAlt(CharacterVector x) {
#ifdef ALTREP_SUPPORTED
  auto ret = new stdvec_data(x.size());
  for(int i=0; i < x.size(); i++) {
    SEXP xi = x[i];
    if(xi == NA_STRING) {
      ret->encodings[i] = 5;
    } else {
      switch(Rf_getCharCE(xi)) {
      case CE_NATIVE:
        ret->encodings[i] = 1;
        ret->strings[i] = Rcpp::as<std::string>(xi);
        break;
      case CE_UTF8:
        ret->encodings[i] = 2;
        ret->strings[i] = Rcpp::as<std::string>(xi);
        break;
      case CE_LATIN1:
        ret->encodings[i] = 3;
        ret->strings[i] = Rcpp::as<std::string>(xi);
        break;
      case CE_BYTES:
        ret->encodings[i] = 4;
        ret->strings[i] = Rcpp::as<std::string>(xi);
        break;
      default:
        ret->encodings[i] = 5;
      break;
      }
    }
  }
  return stdvec_string::Make(ret, true);
#else
  Rcerr << "this function is not available in R < 3.5.0" << std::endl;
  return R_NilValue;
#endif
}

// [[Rcpp::export]]
int openFd(std::string filename, std::string mode) {
  if(mode == "w") {
#ifdef _WIN32
    int fd = open(filename.c_str(), _O_WRONLY | _O_CREAT | O_TRUNC | _O_BINARY, _S_IWRITE);
#else
    int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
#endif
    if(fd == -1) throw std::runtime_error("error creating file descriptor");
    return fd;
  } else if(mode == "r") {
#ifdef _WIN32
    int fd = open(filename.c_str(), O_RDONLY | _O_BINARY);
#else
    int fd = open(filename.c_str(), O_RDONLY);
#endif
    if(fd == -1) throw std::runtime_error("error creating file descriptor");
    return fd;
  } else if(mode == "rw" || mode == "wr") {
#ifdef _WIN32
    int fd = open(filename.c_str(), _O_RDWR | _O_CREAT | O_TRUNC | _O_BINARY, _S_IWRITE);
#else
    int fd = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
#endif
    if(fd == -1) throw std::runtime_error("error creating file descriptor");
    return fd;
  } else {
    throw std::runtime_error("mode should be w or r or rw");
  }
}

// [[Rcpp::export]]
RawVector readFdDirect(int fd, int n_bytes) {
  RawVector x(n_bytes);
  fd_wrapper fw = fd_wrapper(fd);
  fw.read(reinterpret_cast<char*>(RAW(x)), n_bytes);
  return x;
}


// [[Rcpp::export]]
int closeFd(int fd) {
  return close(fd);
}

// [[Rcpp::export]]
SEXP openMmap(int fd, double length) {
#ifdef _WIN32
  throw std::runtime_error("mmap not available on windows");
#elif __APPLE__
  size_t _length = static_cast<size_t>(length);
  void * map = mmap(NULL, _length, PROT_READ, MAP_SHARED, fd, 0);
  return R_MakeExternalPtr(map, R_NilValue, R_NilValue);
#else
  size_t _length = static_cast<size_t>(length);
  void * map = mmap(NULL, _length, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
  return R_MakeExternalPtr(map, R_NilValue, R_NilValue);
#endif
}

// [[Rcpp::export]]
int closeMmap(SEXP map, double length) {
#ifdef _WIN32
  throw std::runtime_error("mmap not available on windows");
#else
  size_t _length = static_cast<size_t>(length);
  void * m = R_ExternalPtrAddr(map);
  return munmap(m, _length);
#endif
}

// [[Rcpp::export]]
SEXP openHandle(std::string filename, std::string mode) {
#ifdef _WIN32
  HANDLE h;
  if(mode == "rw" || mode == "wr") {
    h = CreateFileA(filename.c_str(), GENERIC_WRITE | GENERIC_READ, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
  } else if(mode == "w") {
    h = CreateFileA(filename.c_str(), GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
  } else if(mode == "r") {
    h = CreateFileA(filename.c_str(), GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
  } else {
    throw std::runtime_error("mode should be w or r or rw");
  }
  return R_MakeExternalPtr(h, R_NilValue, R_NilValue);
#else
  throw std::runtime_error("Windows handle only available on windows");
#endif
}

// [[Rcpp::export]]
bool closeHandle(SEXP handle) {
#ifdef _WIN32
  HANDLE h = R_ExternalPtrAddr(handle);
  return CloseHandle(h);
#else
  throw std::runtime_error("Windows handle only available on windows");
#endif
}

// [[Rcpp::export]]
SEXP openWinFileMapping(SEXP handle, double length) {
#ifdef _WIN32
  uint64_t dlen = static_cast<uint64_t>(length);
  DWORD dlen_high = dlen >> 32;
  DWORD dlen_low = dlen & 0x00000000FFFFFFFF;
  HANDLE h = R_ExternalPtrAddr(handle);
  HANDLE fm = CreateFileMappingA(h, NULL, PAGE_READWRITE, dlen_high, dlen_low, NULL);
  return R_MakeExternalPtr(fm, R_NilValue, R_NilValue);
#else
  throw std::runtime_error("Windows file mapping only available on windows");
#endif
}

// [[Rcpp::export]]
SEXP openWinMapView(SEXP handle, double length) {
#ifdef _WIN32
  uint64_t dlen = static_cast<uint64_t>(length);
  HANDLE h = R_ExternalPtrAddr(handle);
  void* map = MapViewOfFile(h, FILE_MAP_ALL_ACCESS, 0, 0, dlen);
  return R_MakeExternalPtr(map, R_NilValue, R_NilValue);
#else
  throw std::runtime_error("Windows file mapping only available on windows");
#endif
}

// [[Rcpp::export]]
bool closeWinMapView(SEXP pointer) {
#ifdef _WIN32
  void* map = R_ExternalPtrAddr(pointer);
  return UnmapViewOfFile(map);
#else
  throw std::runtime_error("Windows file mapping only available on windows");
#endif
}


// std::vector<unsigned char> brotli_compress_raw(RawVector x, int compress_level) {
//   size_t zsize = BrotliEncoderMaxCompressedSize(x.size());
//   uint8_t* xdata = reinterpret_cast<uint8_t*>(RAW(x));
//   std::vector<unsigned char> ret(zsize);
//   uint8_t* retdata = reinterpret_cast<uint8_t*>(ret.data());
//   BrotliEncoderCompress(BROTLI_DEFAULT_QUALITY, BROTLI_DEFAULT_WINDOW, BROTLI_DEFAULT_MODE, 
//                                 x.size(), xdata, &zsize, retdata);
//   ret.resize(zsize);
//   return ret;
// }

// std::vector<unsigned char> brotli_decompress_raw(RawVector x) {
//   size_t available_in = x.size();
//   const uint8_t* next_in = reinterpret_cast<uint8_t*>(RAW(x));
//   std::vector<uint8_t> dbuffer(available_in);
//   size_t available_out = dbuffer.size();
//   uint8_t * next_out = dbuffer.data();
//   std::vector<unsigned char> retdata(0);
//   BrotliDecoderResult result;
//   BrotliDecoderState* state = BrotliDecoderCreateInstance(NULL, NULL, NULL);
//   do {
//     result = BrotliDecoderDecompressStream(state, &available_in, &next_in, &available_out, &next_out, NULL);
//     if(result == BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT) {
//       // error since all the input is available
//     } else if(result == BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT || result == BROTLI_DECODER_RESULT_SUCCESS) {
//       std::cout << BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT << " " << available_out << "\n";
//       retdata.insert(retdata.end(),dbuffer.data(), dbuffer.data() + (dbuffer.size() - available_out));
//       available_out = dbuffer.size();
//       next_out = dbuffer.data();
//     }
//   } while (result != BROTLI_DECODER_RESULT_SUCCESS);
//   BrotliDecoderDestroyInstance(state);
//   return retdata;
// }