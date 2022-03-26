////////////////////////////////////////////////////////////////
// common utility functions for deserialization
////////////////////////////////////////////////////////////////

#ifndef QS_DESERIALIZE_COMMON_H
#define QS_DESERIALIZE_COMMON_H

#include <qs_common.h>

// #define QS_DEBUG

#ifdef QS_DEBUG
std::string qtypestr(qstype x) {
  const std::string enum_strings[] = { 
    "NUMERIC", "INTEGER", "LOGICAL", "CHARACTER", "NIL", "LIST", "COMPLEX", "RAW", "PAIRLIST", "LANG", "CLOS", "PROM", "DOT", "SYM",
    "PAIRLIST_WF", "LANG_WF", "CLOS_WF", "PROM_WF", "DOT_WF",
    "S4", "S4FLAG", "LOCKED_ENV", "UNLOCKED_ENV", "REFERENCE",
    "ATTRIBUTE", "RSERIALIZED" };
  return enum_strings[(int)x];
}
#endif


inline void readHeader_common(qstype & object_type, uint64_t & r_array_len, uint64_t & data_offset, const char * const header) {
  uint8_t hd = reinterpret_cast<const uint8_t*>(header)[data_offset];
  switch(hd) {
  case extension_header:
  {
    hd = reinterpret_cast<const uint8_t*>(header)[data_offset+1];
    switch(hd) {
    case s4flag_header:
      data_offset += 2;
      object_type = qstype::S4FLAG;
      return;
    case s4_header:
      data_offset += 2;
      object_type = qstype::S4;
      return;
    case pairlist_header:
      r_array_len = unaligned_cast<uint32_t>(header, data_offset+2); 
      data_offset += 6;
      object_type = qstype::PAIRLIST;
      return;
    case clos_header:
      data_offset += 2;
      object_type = qstype::CLOS;
      return;
    case lang_header:
      data_offset += 2;
      object_type = qstype::LANG;
      return;
    case prom_header:
      data_offset += 2;
      object_type = qstype::PROM;
      return;
    case dot_header:
      data_offset += 2;
      object_type = qstype::DOT;
      return;
    case pairlist_wf_header:
      r_array_len = unaligned_cast<uint32_t>(header, data_offset+2); 
      data_offset += 6;
      object_type = qstype::PAIRLIST_WF;
      return;
    case clos_wf_header:
      r_array_len = unaligned_cast<uint32_t>(header, data_offset+2); 
      data_offset += 6;
      object_type = qstype::CLOS_WF;
      return;
    case lang_wf_header:
      r_array_len = unaligned_cast<uint32_t>(header, data_offset+2); 
      data_offset += 6;
      object_type = qstype::LANG_WF;
      return;
    case prom_wf_header:
      r_array_len = unaligned_cast<uint32_t>(header, data_offset+2); 
      data_offset += 6;
      object_type = qstype::PROM_WF;
      return;
    case dot_wf_header:
      r_array_len = unaligned_cast<uint32_t>(header, data_offset+2); 
      data_offset += 6;
      object_type = qstype::DOT_WF;
      return;
    case unlocked_env_header:
      r_array_len = unaligned_cast<uint32_t>(header, data_offset+2); 
      data_offset += 6;
      object_type = qstype::UNLOCKED_ENV;
      return;
    case locked_env_header:
      r_array_len = unaligned_cast<uint32_t>(header, data_offset+2); 
      data_offset += 6;
      object_type = qstype::LOCKED_ENV;
      return;
    case reference_object_header:
      r_array_len = unaligned_cast<uint32_t>(header, data_offset+2); 
      data_offset += 6;
      object_type = qstype::REFERENCE;
      return;
    }
  }
  case sym_header:
    data_offset += 1;
    object_type = qstype::SYM;
    return;
  case numeric_header_8:
    r_array_len =  *reinterpret_cast<const uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = qstype::NUMERIC;
    return;
  case numeric_header_16:
    r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
    data_offset += 3;
    object_type = qstype::NUMERIC;
    return;
  case numeric_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = qstype::NUMERIC;
    return;
  case numeric_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = qstype::NUMERIC;
    return;
  case list_header_8:
    r_array_len =  *reinterpret_cast<const uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = qstype::LIST;
    return;
  case list_header_16:
    r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
    data_offset += 3;
    object_type = qstype::LIST;
    return;
  case list_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = qstype::LIST;
    return;
  case list_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = qstype::LIST;
    return;
  case integer_header_8:
    r_array_len =  *reinterpret_cast<const uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = qstype::INTEGER;
    return;
  case integer_header_16:
    r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
    data_offset += 3;
    object_type = qstype::INTEGER;
    return;
  case integer_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = qstype::INTEGER;
    return;
  case integer_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = qstype::INTEGER;
    return;
  case logical_header_8:
    r_array_len =  *reinterpret_cast<const uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = qstype::LOGICAL;
    return;
  case logical_header_16:
    r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
    data_offset += 3;
    object_type = qstype::LOGICAL;
    return;
  case logical_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = qstype::LOGICAL;
    return;
  case logical_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = qstype::LOGICAL;
    return;
  case raw_header_32:
    r_array_len = unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = qstype::RAW;
    return;
  case raw_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = qstype::RAW;
    return;
  case character_header_8:
    r_array_len =  *reinterpret_cast<const uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = qstype::CHARACTER;
    return;
  case character_header_16:
    r_array_len = unaligned_cast<uint16_t>(header, data_offset+1) ;
    data_offset += 3;
    object_type = qstype::CHARACTER;
    return;
  case character_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = qstype::CHARACTER;
    return;
  case character_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = qstype::CHARACTER;
    return;
  case complex_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = qstype::COMPLEX;
    return;
  case complex_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = qstype::COMPLEX;
    return;
  case null_header:
    r_array_len =  0;
    data_offset += 1;
    object_type = qstype::NIL;
    return;
  case attribute_header_8:
    r_array_len =  *reinterpret_cast<const uint8_t*>(header+data_offset+1) ;
    data_offset += 2;
    object_type = qstype::ATTRIBUTE;
    return;
  case attribute_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = qstype::ATTRIBUTE;
    return;
  case nstype_header_32:
    r_array_len =  unaligned_cast<uint32_t>(header, data_offset+1) ;
    data_offset += 5;
    object_type = qstype::RSERIALIZED;
    return;
  case nstype_header_64:
    r_array_len =  unaligned_cast<uint64_t>(header, data_offset+1) ;
    data_offset += 9;
    object_type = qstype::RSERIALIZED;
    return;
  }
  uint8_t h5 = reinterpret_cast<const uint8_t *>(header)[data_offset] & 0xE0;
  switch(h5) {
  case numeric_header_5:
    r_array_len = *reinterpret_cast<const uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = qstype::NUMERIC;
    return;
  case list_header_5:
    r_array_len = *reinterpret_cast<const uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = qstype::LIST;
    return;
  case integer_header_5:
    r_array_len = *reinterpret_cast<const uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = qstype::INTEGER;
    return;
  case logical_header_5:
    r_array_len = *reinterpret_cast<const uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = qstype::LOGICAL;
    return;
  case character_header_5:
    r_array_len = *reinterpret_cast<const uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = qstype::CHARACTER;
    return;
  case attribute_header_5:
    r_array_len = *reinterpret_cast<const uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    object_type = qstype::ATTRIBUTE;
    return;
  }
  // additional types
  throw std::runtime_error("something went wrong (reading object header)");
}
inline void readStringHeader_common(uint32_t & r_string_len, cetype_t & ce_enc, uint64_t & data_offset, const char * const header) {
  uint8_t enc = reinterpret_cast<const uint8_t*>(header)[data_offset] & 0xC0;
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
  
  if((reinterpret_cast<const uint8_t*>(header)[data_offset] & 0x20) == string_header_5) {
    r_string_len = *reinterpret_cast<const uint8_t*>(header+data_offset) & 0x1F ;
    data_offset += 1;
    return;
  } else {
    uint8_t hd = reinterpret_cast<const uint8_t*>(header)[data_offset] & 0x1F;
    switch(hd) {
    case string_header_8:
      r_string_len =  *reinterpret_cast<const uint8_t*>(header+data_offset+1) ;
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
  throw std::runtime_error("something went wrong (reading string header)");
}
inline void readFlags_common(int & packed_flags, uint64_t & data_offset, const char * const header) {
  packed_flags = unaligned_cast<int>(header, data_offset);
  data_offset += 4;
}

template <class T>
SEXP processBlock(T * const sobj) {
  qstype obj_type;
  uint64_t r_array_len;
  uint64_t number_of_attributes = 0;
  bool s4_flag = false;
  sobj->readHeader(obj_type, r_array_len);
#ifdef QS_DEBUG
  std::cout << qtypestr(obj_type) << " " << r_array_len << std::endl;
#endif
  if(obj_type == qstype::S4FLAG) {
    s4_flag = true;
    sobj->readHeader(obj_type, r_array_len);
#ifdef QS_DEBUG
    std::cout << qtypestr(obj_type) << " " << r_array_len << std::endl;
#endif
  }
  if(obj_type == qstype::ATTRIBUTE) {
    number_of_attributes = r_array_len;
    sobj->readHeader(obj_type, r_array_len);
#ifdef QS_DEBUG
    std::cout << qtypestr(obj_type) << " " << r_array_len << std::endl;
#endif
  }
  SEXP obj;
  Protect_Tracker pt = Protect_Tracker();
  switch(obj_type) {
  case qstype::REFERENCE:
    return sobj->object_ref_hash.at(static_cast<uint32_t>(r_array_len));
  case qstype::PAIRLIST:
  {
    obj = PROTECT(Rf_allocList(r_array_len)); pt++;
    SEXP obj_i = obj;
    for(uint64_t i=0; i<r_array_len; i++) {
      uint32_t r_string_len;
      cetype_t string_encoding;
      sobj->readStringHeader(r_string_len, string_encoding);
#ifdef QS_DEBUG
      std::cout << "pairlist name string " << r_string_len << " " << (int)string_encoding << std::endl;
#endif
      if(r_string_len != NA_STRING_LENGTH) {
        std::string temp_tag_string;
        temp_tag_string.resize(r_string_len);
        sobj->getBlockData(&temp_tag_string[0], r_string_len);
        SET_TAG(obj_i, Rf_install(temp_tag_string.c_str()));
      }
      SETCAR(obj_i, processBlock(sobj));
      obj_i = CDR(obj_i);
    }
  }
    break;
  case qstype::LANG:
  case qstype::CLOS:
  case qstype::PROM:
  case qstype::DOT:
  {
    switch(obj_type) {
    case qstype::LANG:
      PROTECT(obj = Rf_allocSExp(LANGSXP)); pt++;
      break;
    case qstype::CLOS:
      PROTECT(obj = Rf_allocSExp(CLOSXP)); pt++;
      break;
    case qstype::PROM:
      PROTECT(obj = Rf_allocSExp(PROMSXP)); pt++;
      break;
    case qstype::DOT:
      PROTECT(obj = Rf_allocSExp(DOTSXP)); pt++;
      break;
    default:
      throw std::runtime_error("."); // add default to hush compiler warnings
    }
    SET_TAG(obj, processBlock(sobj));
    SETCAR(obj, processBlock(sobj));
    SETCDR(obj, processBlock(sobj));
    if(obj_type == qstype::CLOS && CLOENV(obj) == R_NilValue) {
      SET_CLOENV(obj, R_BaseEnv);
    } else if(obj_type == qstype::PROM && PRENV(obj) == R_NilValue) {
      SET_PRENV(obj, R_BaseEnv);
    }
  }
    break;
  case qstype::PAIRLIST_WF:
  {
    // unpack_pods(reinterpret_cast<char*>(&r_array_len), flags, mlen);
    obj = PROTECT(Rf_allocList(r_array_len)); pt++;
    SEXP obj_i = obj;
    std::string temp_tag_string;
    for(uint64_t i=0; i<r_array_len; i++) {
      int packed_flags;
      sobj->readFlags(packed_flags);
      uint32_t r_string_len;
      cetype_t string_encoding;
      sobj->readStringHeader(r_string_len, string_encoding);
#ifdef QS_DEBUG
      std::cout << "pairlist name string " << r_string_len << " " << (int)string_encoding << std::endl;
#endif
      if(r_string_len != NA_STRING_LENGTH) {
        temp_tag_string.resize(r_string_len);
        sobj->getBlockData(&temp_tag_string[0], r_string_len);
        SET_TAG(obj_i, Rf_install(temp_tag_string.c_str()));
      }
      SETCAR(obj_i, processBlock(sobj));
      unpackFlags(obj_i, packed_flags);
      obj_i = CDR(obj_i);
    }
  }
    break;
  case qstype::LANG_WF:
  case qstype::CLOS_WF:
  case qstype::PROM_WF:
  case qstype::DOT_WF:
  {
    switch(obj_type) {
    case qstype::LANG_WF:
      PROTECT(obj = Rf_allocSExp(LANGSXP)); pt++;
      break;
    case qstype::CLOS_WF:
      PROTECT(obj = Rf_allocSExp(CLOSXP)); pt++;
      break;
    case qstype::PROM_WF:
      PROTECT(obj = Rf_allocSExp(PROMSXP)); pt++;
      break;
    case qstype::DOT_WF:
      PROTECT(obj = Rf_allocSExp(DOTSXP)); pt++;
      break;
    default:
      throw std::runtime_error("."); // add default to hush compiler warnings
    }
    SET_TAG(obj, processBlock(sobj));
    SETCAR(obj, processBlock(sobj));
    SETCDR(obj, processBlock(sobj));
    if(obj_type == qstype::CLOS && CLOENV(obj) == R_NilValue) {
      SET_CLOENV(obj, R_BaseEnv);
    } else if(obj_type == qstype::PROM && PRENV(obj) == R_NilValue) {
      SET_PRENV(obj, R_BaseEnv);
    }
    unpackFlags(obj, static_cast<int>(r_array_len));
  }
    break;
  case qstype::UNLOCKED_ENV:
  case qstype::LOCKED_ENV:
  {
    obj = PROTECT(Rf_allocSExp(ENVSXP)); pt++;
    sobj->object_ref_hash.emplace(static_cast<uint32_t>(r_array_len), obj);
    SET_ENCLOS(obj, processBlock(sobj));
    SET_FRAME(obj, processBlock(sobj));
    SET_HASHTAB(obj, processBlock(sobj));
    // R_RestoreHashCount(obj); // doesn't exist in new API; the function sets truelength to the number of filled hash slots
    SEXP table = HASHTAB(obj);
    if(table != R_NilValue) {
      int size = Rf_xlength(table);
      int count = 0;
      for(int i = 0; i < size; ++i) {
        if(VECTOR_ELT(table, i) != R_NilValue) ++count;
      }
      SET_TRUELENGTH(table, count);
    }
    if(obj_type == qstype::LOCKED_ENV) R_LockEnvironment(obj, FALSE);
    if(ENCLOS(obj) == R_NilValue) SET_ENCLOS(obj, R_BaseEnv);
  }
    break;
  case qstype::S4:
    // obj = PROTECT(Rf_allocS4Object()); pt++; // S4 object may not have S4 flag
    obj = PROTECT(Rf_allocSExp(S4SXP)); pt++;
    break;
  case qstype::LIST: 
    obj = PROTECT(Rf_allocVector(VECSXP, r_array_len)); pt++;
    for(uint64_t i=0; i<r_array_len; i++) {
      SET_VECTOR_ELT(obj, i, processBlock(sobj));
    }
    break;
  case qstype::NUMERIC:
    obj = PROTECT(Rf_allocVector(REALSXP, r_array_len)); pt++;
    if(sobj->qm.real_shuffle) {
      sobj->getShuffleBlockData(reinterpret_cast<char*>(REAL(obj)), r_array_len*8, 8);
    } else {
      sobj->getBlockData(reinterpret_cast<char*>(REAL(obj)), r_array_len*8);
    }
    break;
  case qstype::INTEGER:
    obj = PROTECT(Rf_allocVector(INTSXP, r_array_len)); pt++;
    if(sobj->qm.int_shuffle) {
      sobj->getShuffleBlockData(reinterpret_cast<char*>(INTEGER(obj)), r_array_len*4, 4);
    } else {
      sobj->getBlockData(reinterpret_cast<char*>(INTEGER(obj)), r_array_len*4);
    }
    break;
  case qstype::LOGICAL:
    obj = PROTECT(Rf_allocVector(LGLSXP, r_array_len)); pt++;
    if(sobj->qm.lgl_shuffle) {
      sobj->getShuffleBlockData(reinterpret_cast<char*>(LOGICAL(obj)), r_array_len*4, 4);
    } else {
      sobj->getBlockData(reinterpret_cast<char*>(LOGICAL(obj)), r_array_len*4);
    }
    break;
  case qstype::COMPLEX:
    obj = PROTECT(Rf_allocVector(CPLXSXP, r_array_len)); pt++;
    if(sobj->qm.cplx_shuffle) {
      sobj->getShuffleBlockData(reinterpret_cast<char*>(COMPLEX(obj)), r_array_len*16, 8);
    } else {
      sobj->getBlockData(reinterpret_cast<char*>(COMPLEX(obj)), r_array_len*16);
    }
    break;
  case qstype::RAW:
    obj = PROTECT(Rf_allocVector(RAWSXP, r_array_len)); pt++;
    if(r_array_len > 0) sobj->getBlockData(reinterpret_cast<char*>(RAW(obj)), r_array_len);
    break;
  case qstype::CHARACTER:
#ifdef USE_ALT_REP
    if(sobj->use_alt_rep_bool) {
      obj = PROTECT(sf_vector(r_array_len)); pt++;
      auto & ref = sf_vec_data_ref(obj);
      for(uint64_t i=0; i < r_array_len; i++) {
        uint32_t r_string_len;
        cetype_t string_encoding;
        sobj->readStringHeader(r_string_len, string_encoding);
#ifdef QS_DEBUG
        std::cout << "string " << r_string_len << " " << (int)string_encoding << " ";
#endif
        if(r_string_len == NA_STRING_LENGTH) {
          ref[i] = sfstring(NA_STRING);
        } else {
          if(r_string_len == 0) {
            ref[i] = sfstring();
          } else {
            ref[i] = sfstring(r_string_len);
            sobj->getBlockData(&ref[i].sdata[0], r_string_len);
            ref[i].check_if_native_is_ascii(string_encoding);
#ifdef QS_DEBUG
            std::cout << ref[i].sdata;
#endif
          }
        }
#ifdef QS_DEBUG
        std::cout << std::endl;
#endif
      }
    } else {
#endif
      obj = PROTECT(Rf_allocVector(STRSXP, r_array_len)); pt++;
      for(uint64_t i=0; i<r_array_len; i++) {
        uint32_t r_string_len;
        cetype_t string_encoding;
        sobj->readStringHeader(r_string_len, string_encoding);
#ifdef QS_DEBUG
        std::cout << "string " << r_string_len << " " << (int)string_encoding << std::endl;
#endif
        if(r_string_len == NA_STRING_LENGTH) {
          SET_STRING_ELT(obj, i, NA_STRING);
        } else if(r_string_len == 0) {
          SET_STRING_ELT(obj, i, Rf_mkCharLen("", 0));
        } else if(r_string_len > 0) {
          if(r_string_len > sobj->temp_string.size()) {
            sobj->temp_string.resize(r_string_len);
          }
          sobj->getBlockData(&sobj->temp_string[0], r_string_len);
          SET_STRING_ELT(obj, i, Rf_mkCharLenCE(sobj->temp_string.data(), r_string_len, string_encoding));
        }
      }
#ifdef USE_ALT_REP
    }
#endif
    break;
  case qstype::SYM:
  {
    uint32_t r_string_len;
    cetype_t string_encoding;
    sobj->readStringHeader(r_string_len, string_encoding);
    // symbols cannot be NA or zero length
    if(r_string_len > sobj->temp_string.size()) {
      sobj->temp_string.resize(r_string_len);
    }
    sobj->getBlockData(&sobj->temp_string[0], r_string_len);
#ifdef QS_DEBUG
    std::cout << "sym string " << r_string_len << " " << (int)string_encoding << " "  << std::string(sobj->temp_string.data(), r_string_len) << std::endl;
#endif
    obj = PROTECT(Rf_mkCharLenCE(sobj->temp_string.data(), r_string_len, string_encoding)); pt++;
    obj = Rf_installChar(obj); //Rf_installTrChar in R 4.0.0
  }
  break;
  case qstype::RSERIALIZED:
  {
    SEXP obj_data = PROTECT(Rf_allocVector(RAWSXP, r_array_len)); pt++;
    sobj->getBlockData(reinterpret_cast<char*>(RAW(obj_data)), r_array_len);
    obj = PROTECT(unserializeFromRaw(obj_data)); pt++;
    return obj;
  }
  default: // also NILSXP
    obj = R_NilValue;
    return obj;
  }
  if(number_of_attributes > 0) {
    SEXP attrib_pairlist = PROTECT(Rf_allocList(number_of_attributes)); pt++;
    SEXP aptr = attrib_pairlist;
    std::string temp_attribute_string;
    for(uint64_t i=0; i<number_of_attributes; i++) {
      uint32_t r_string_len;
      cetype_t string_encoding;
      sobj->readStringHeader(r_string_len, string_encoding);
      temp_attribute_string.resize(r_string_len);
      sobj->getBlockData(&temp_attribute_string[0], r_string_len);
#ifdef QS_DEBUG
      std::cout << "attr string " << r_string_len << " " << (int)string_encoding << " "  << temp_attribute_string << std::endl;
#endif
      // Is protect needed here?  
      // I believe it is not, since SET_TAG/SETCAR shouldn't allocate and serialize.c doesn't protect either
      // What about IS_CHARACTER?
      SET_TAG(aptr, Rf_install(temp_attribute_string.c_str()));
      if(temp_attribute_string == "class") {
        SEXP aobj = PROTECT(processBlock(sobj)); pt++;
        if((IS_CHARACTER(aobj)) & (Rf_xlength(aobj) >= 1)) {
          SET_OBJECT(obj, 1);
        }
        SETCAR(aptr, aobj);
      } else {
        SETCAR(aptr, processBlock(sobj));
      }
      aptr = CDR(aptr);
    }
    SET_ATTRIB(obj, attrib_pairlist);
  }
  if(s4_flag) {
    SET_S4_OBJECT(obj);
    // SET_OBJECT(obj, 1); // this flag seems kind of pointless
  }
  return obj;
}

#endif
