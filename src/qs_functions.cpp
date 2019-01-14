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

#include "qs_header.h"

// [[Rcpp::export]]
int zstd_compressBound(int size) {
  return ZSTD_compressBound(size);
}

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
SEXP qread(std::string file) {
  std::ifstream myFile(file, std::ios::in | std::ios::binary);
  Data_Context dc = Data_Context(myFile);
  return dc.processBlock();
}

// [[Rcpp::export]]
RObject qdump(std::string file) {
  std::ifstream myFile(file.c_str(), std::ios::in | std::ios::binary);
  std::array<char,4> reserve_bits;
  myFile.read(reserve_bits.data(),4);
  char sys_endian = is_big_endian() ? 1 : 0;
  if(reserve_bits[3] != sys_endian) throw exception("Endian of system doesn't match file endian");
  
  uint64_t number_of_blocks = readSizeFromFile8(myFile);
  // std::cout << number_of_blocks << "\n";
  std::vector< std::pair<char*, uint64_t> > block_pointers(number_of_blocks);
  std::array<char,4> zsize_ar;
  uint64_t block_size;
  uint64_t zsize;
  std::vector<char> zblock(ZSTD_compressBound(BLOCKSIZE));
  std::vector<char> block(BLOCKSIZE);
  List ret = List(number_of_blocks);
  for(uint64_t i=0; i<number_of_blocks; i++) {
    myFile.read(zsize_ar.data(), 4);
    zsize = *reinterpret_cast<uint32_t*>(zsize_ar.data());
    // std::cout << zsize << "\n";
    myFile.read(zblock.data(), zsize);
    block_size = ZSTD_decompress(block.data(), BLOCKSIZE, zblock.data(), zsize);
    ret[i] = RawVector(block.begin(), block.begin() + block_size);
  }
  return ret;
}


// [[Rcpp::export]]
void qsave(RObject x, std::string file, int compress_level=-1) {
  std::ofstream myFile(file.c_str(), std::ios::out | std::ios::binary);
  std::array<char,4> reserve_bits = {0,0,0,0};
  reserve_bits[3] = is_big_endian() ? 1 : 0;
  myFile.write(reserve_bits.data(),4); // some reserve bits for future use
  writeSizeToFile8(myFile, 0); // number of compressed blocks
  CompressBuffer vbuf(myFile, compress_level);
  appendToVbuf(vbuf, x);
  vbuf.flush();
  myFile.seekp(4);
  writeSizeToFile8(myFile, vbuf.number_of_blocks);
  myFile.close();
}

// [[Rcpp::export]]
void qs_use_alt_rep(bool s) {
  use_alt_rep_bool = s;
}

// [[Rcpp::export]]
void qs_set_blocksize(int s) {
  BLOCKSIZE = intToSize(s);
}

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

// [[Rcpp::export]]
std::vector<unsigned char> zstd_compress_raw(RawVector x, int compress_level) {
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
  RawVector ret(zsize);
  char* retdata = reinterpret_cast<char*>(RAW(ret));
  ZSTD_decompress(retdata, retsize, xdata, zsize);
  return ret;
}



// [[Rcpp::export]]
SEXP convertToAlt(CharacterVector x) {
  auto ret = new stdvec_data(x.size());
  for(uint64_t i=0; i < x.size(); i++) {
    SEXP xi = x[i];
    if(xi == NA_STRING) {
      ret->encodings[i] = '\5';
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
}
