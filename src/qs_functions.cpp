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
RObject qread(std::string file) {
  std::ifstream myFile(file.c_str(), std::ios::in | std::ios::binary);
  std::array<char,4> reserve_bits = {0,0,0,0};
  myFile.read(reserve_bits.data(),4);
  char sys_endian = is_big_endian() ? 1 : 0;
  if(reserve_bits[3] != sys_endian) throw exception("Endian of system doesn't match file endian");
  
  size_t number_of_blocks = readSizeFromFile8(myFile);
  
  std::vector< std::pair<char*, size_t> > block_pointers(number_of_blocks);
  std::array<char,4> zsize_ar = {0,0,0,0};
  size_t block_size;
  size_t zsize;
  std::vector<char> zblock(ZSTD_compressBound(BLOCKSIZE));
  std::vector<char> block(BLOCKSIZE);
  std::vector<String_Future> fstrings;
  std::vector<ObjNode*> fattributes; // attribute futures
  
  // singleton objects for OMP ordered section that persist through loop
  std::unique_ptr<ObjNode> root_node = std::make_unique<ObjNode>();
  root_node->parent = root_node.get();
  ObjNode * current_node = root_node.get();
  SEXPTYPE obj_type;
  RObject next_obj;
  // CharacterVector next_cv; // should be equal to next_obj for Character Vectors -- helper variable to avoid tons of ROjbect to CharacterVector casts
  size_t r_array_len = 0; // persistence only important for STRSXP
  size_t number_of_strings_processed = 0;  // for STRSXP/CHARSXP
  std::string temp_string(256, '\0'); // temporary string to fill for R String constructor -- local conceptually, but should be faster to avoid too many re-allocs?
  char* outp = &temp_string[0];  // this could be local too -- output buffer for atomics, strings
  size_t data_offset = 0; // offset of current block
  
  for(size_t i=0; i<number_of_blocks; i++) {
    // Read compressed data
    myFile.read(zsize_ar.data(), 4);
    zsize = *reinterpret_cast<uint32_t*>(zsize_ar.data());
    myFile.read(zblock.data(), zsize);
    if(block_pointers[i].first == nullptr || block_pointers[i].second != BLOCKSIZE) {
      block_size = ZSTD_decompress(block.data(), BLOCKSIZE, zblock.data(), zsize);
    } else {
      block_size = ZSTD_decompress(block_pointers[i].first, block_pointers[i].second, zblock.data(), zsize);
      continue;
    }
    
    // Here will be the OMP ordered section -- process block assigned to temporary block buffer
    
    // If block fully belongs to previous R object -- memcpy into previous R object and go to next block
    if(block_pointers[i].first != nullptr && block_pointers[i].second == BLOCKSIZE) {
      memcpy(block_pointers[i].first, block.data(), block_pointers[i].second);
      continue;
    } 
    
    if(block_pointers[i].first != nullptr) {
      memcpy(block_pointers[i].first, block.data(), block_pointers[i].second);
      data_offset = block_pointers[i].second;
    } else {
      data_offset = 0;
    }
    
    // parse the remaining part of the block, starting from offset
    // there may be objects fully within a block (contained objects)
    // parse all contained objects, then set block_pointers for subsequent blocks
    while(data_offset < block_size) {
      // std::cout << number_of_strings_processed << " " << r_array_len << "\n";
      if(number_of_strings_processed < r_array_len && obj_type == STRSXP) {
        uint32_t r_string_len;
        cetype_t string_encoding;
        readStringHeader(block.data(), r_string_len, string_encoding, data_offset);
        if(r_string_len == NA_STRING_LENGTH) {
          // next_cv[number_of_strings_processed] = NA_STRING;
          SET_STRING_ELT(next_obj, number_of_strings_processed, NA_STRING);
        } else if(r_string_len == 0) {
          // next_cv[number_of_strings_processed-1] = Rf_mkCharLen("", 0);
          SET_STRING_ELT(next_obj, number_of_strings_processed, Rf_mkCharLen("", 0));
        } else if(r_string_len > 0) {
          if(block_size - data_offset >= r_string_len ) {
            if(r_string_len > temp_string.size()) {
              temp_string.resize(r_string_len);
            }
            outp = &temp_string[0];
            memcpy(outp,block.data()+data_offset, r_string_len);
            data_offset += r_string_len;
            SET_STRING_ELT(next_obj, number_of_strings_processed, Rf_mkCharLenCE(outp, r_string_len, string_encoding));
          } else {
            String_Future sf = String_Future(next_obj, number_of_strings_processed, r_string_len, string_encoding);
            outp = sf.dataPtr();
            
            setBlockPointers(outp, block, data_offset, block_pointers, block_size, r_string_len, 1, i);
            
            fstrings.push_back(std::move(sf));
          }
        }
        number_of_strings_processed++;
      } else if( (TYPEOF(current_node->obj) != VECSXP || current_node->remaining_children == 0) &&
                 current_node->remaining_attributes != 0 && 
                 current_node->attributes.size() == current_node->attributes_names.size() ) {
        // ^ if next object is an attribute name = if all children processed && # of attributes processed < total # of attributes and attribute names size == attributes size
        uint32_t r_string_len;
        cetype_t string_encoding;
        readStringHeader(block.data(), r_string_len, string_encoding, data_offset);
        if(r_string_len == 0 || r_string_len == NA_STRING_LENGTH) throw exception("attribute name should be a non-zero/non-null string");
        current_node->attributes_names.push_back(std::string(r_string_len, '\0'));
        outp = &current_node->attributes_names.back()[0];
        if(block_size - data_offset >= r_string_len ) {
          memcpy(outp,block.data()+data_offset, r_string_len);
          data_offset += r_string_len;
        } else {
          setBlockPointers(outp, block, data_offset, block_pointers, block_size, r_string_len, 1, i); 
        }
      } else {
        readHeader(block.data(), obj_type, r_array_len, data_offset);
        size_t attribute_length = 0;
        if(obj_type == ANYSXP) {
          attribute_length = r_array_len;
          readHeader(block.data(), obj_type, r_array_len, data_offset);
        }
        next_obj = addNewNodeToParent(current_node, obj_type, r_array_len, attribute_length);
        if(obj_type == VECSXP) {
          // do nothing
        } else if(obj_type == STRSXP) {
          number_of_strings_processed = 0;
          // next_cv = CharacterVector(next_obj); // do we really need this?
        } else { // all "Atomic" R types go here -- numeric, integer, logical vectors and String and NILSXP
          if(r_array_len == 0) { continue; }
          size_t type_char_width;
          outp = getObjDataPointer(next_obj, obj_type, type_char_width);
          if(block_size - data_offset >= (r_array_len * type_char_width) ) {
            memcpy(outp,block.data()+data_offset, r_array_len * type_char_width);
            data_offset += r_array_len * type_char_width;
          } else {
            
            setBlockPointers(outp, block, data_offset, block_pointers, block_size, r_array_len, type_char_width, i);
            
          }
        }
        if(attribute_length > 0) fattributes.push_back(current_node);
      }
    }
    // end OMP ordered section
  }
  // if parse all String_Future objects
  for(size_t q=0; q<fstrings.size(); q++) {
    fstrings[q].push_string();
  }
  // append all attributes to the correct object
  for(size_t q=0; q<fattributes.size(); q++) {
    for(size_t v=0; v<fattributes[q]->attributes_names.size(); v++) {
      if(fattributes[q]->attributes[v]->obj != R_NilValue) {
        fattributes[q]->obj.attr(fattributes[q]->attributes_names[v]) = fattributes[q]->attributes[v]->obj;
      }
    }
  }
  
  myFile.close();
  return root_node->children[0]->obj;
}


// [[Rcpp::export]]
RObject qdump(std::string file) {
  std::ifstream myFile(file.c_str(), std::ios::in | std::ios::binary);
  std::array<char,4> reserve_bits;
  myFile.read(reserve_bits.data(),4);
  char sys_endian = is_big_endian() ? 1 : 0;
  if(reserve_bits[3] != sys_endian) throw exception("Endian of system doesn't match file endian");
  
  size_t number_of_blocks = readSizeFromFile8(myFile);
  // std::cout << number_of_blocks << "\n";
  std::vector< std::pair<char*, size_t> > block_pointers(number_of_blocks);
  std::array<char,4> zsize_ar;
  size_t block_size;
  size_t zsize;
  std::vector<char> zblock(ZSTD_compressBound(BLOCKSIZE));
  std::vector<char> block(BLOCKSIZE);
  List ret = List(number_of_blocks);
  for(size_t i=0; i<number_of_blocks; i++) {
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
void qs_show_warnings(bool s) {
  show_warnings = s;
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
  size_t zsize = ZSTD_compressBound(x.size());
  char* xdata = reinterpret_cast<char*>(RAW(x));
  std::vector<unsigned char> ret(zsize);
  char* retdata = reinterpret_cast<char*>(ret.data());
  zsize = ZSTD_compress(retdata, zsize, xdata, x.size(), compress_level);
  ret.resize(zsize);
  return ret;
}

// [[Rcpp::export]]
RawVector zstd_decompress_raw(RawVector x) {
  size_t zsize = x.size();
  char* xdata = reinterpret_cast<char*>(RAW(x));
  size_t retsize = ZSTD_getFrameContentSize(xdata, x.size());
  RawVector ret(zsize);
  char* retdata = reinterpret_cast<char*>(RAW(ret));
  ZSTD_decompress(retdata, retsize, xdata, zsize);
  return ret;
}