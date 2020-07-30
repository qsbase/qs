////////////////////////////////////////////////////////////////
// common utility functions for serialization
////////////////////////////////////////////////////////////////

#ifndef QS_SERIALIZE_COMMON_H
#define QS_SERIALIZE_COMMON_H

#include <qs_common.h>

struct CountToObjectMap {
  uint32_t index = 0;
  std::unordered_map<SEXP, uint32_t> map;
  inline void add_to_hash(SEXP x) {
    index++; // hash starts at 1
    map.emplace(x, index);
  }
};

// should this be defined in the stringfish package instead?
bool is_unmaterialized_sf_vector(const SEXP obj) {
  if(!ALTREP(obj)) return false;
  SEXP pclass = ATTRIB(ALTREP_CLASS(obj));
  const char * classname = CHAR(PRINTNAME(CAR(pclass)));
  if(std::strcmp( classname, "__sf_vec__") != 0) return false;
  if(DATAPTR_OR_NULL(obj) == nullptr) {
    return true;
  } else {
    return false;
  }
}

template <class T>
void writeHeader_common(const qstype object_type, const uint64_t length, T * const sobj) {
  switch(object_type) {
  case qstype::SYM:
    sobj->push_pod_noncontiguous(sym_header);
    return;
  case qstype::S4:
    // sobj->push_pod_noncontiguous(extension_header, s4_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(s4_header);
    return;
  case qstype::PAIRLIST:
    // sobj->push_pod_noncontiguous(extension_header, pairlist_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(pairlist_header);
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    return;
  case qstype::CLOS:
    // sobj->push_pod_noncontiguous(extension_header, clos_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(clos_header);
    return;
  case qstype::LANG:
    // sobj->push_pod_noncontiguous(extension_header, lang_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(lang_header);
    return;
  case qstype::PROM:
    // sobj->push_pod_noncontiguous(extension_header, prom_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(prom_header);
    return;
  case qstype::DOT:
    // sobj->push_pod_noncontiguous(extension_header, dot_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(dot_header);
    return;
  case qstype::PAIRLIST_WF:
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(pairlist_wf_header);
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    return;
  case qstype::CLOS_WF:
    // sobj->push_pod_noncontiguous(extension_header, clos_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(clos_wf_header);
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    return;
  case qstype::LANG_WF:
    // sobj->push_pod_noncontiguous(extension_header, lang_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(lang_wf_header);
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    return;
  case qstype::PROM_WF:
    // sobj->push_pod_noncontiguous(extension_header, prom_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(prom_wf_header);
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    return;
  case qstype::DOT_WF:
    // sobj->push_pod_noncontiguous(extension_header, dot_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(dot_wf_header);
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    return;
  case qstype::NUMERIC:
    if(length < 32) {
      sobj->push_pod_noncontiguous( static_cast<uint8_t>(numeric_header_5 | static_cast<uint8_t>(length)) );
    } else if(length < 256) { 
      sobj->push_pod_noncontiguous(numeric_header_8);
      sobj->push_pod_contiguous( static_cast<uint8_t>(length) );
    } else if(length < 65536) { 
      sobj->push_pod_noncontiguous(numeric_header_16);
      sobj->push_pod_contiguous( static_cast<uint16_t>(length) );
    } else if(length < 4294967296) {
      sobj->push_pod_noncontiguous(numeric_header_32);
      sobj->push_pod_contiguous( static_cast<uint32_t>(length) );
    } else {
      sobj->push_pod_noncontiguous(numeric_header_64);
      sobj->push_pod_contiguous(length);
    }
    return;
  case qstype::LIST:
    if(length < 32) {
      sobj->push_pod_noncontiguous( static_cast<uint8_t>(list_header_5 | static_cast<uint8_t>(length)) );
    } else if(length < 256) { 
      sobj->push_pod_noncontiguous(list_header_8);
      sobj->push_pod_contiguous(static_cast<uint8_t>(length) );
    } else if(length < 65536) { 
      sobj->push_pod_noncontiguous(list_header_16);
      sobj->push_pod_contiguous(static_cast<uint16_t>(length) );
    } else if(length < 4294967296) {
      sobj->push_pod_noncontiguous(list_header_32);
      sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    } else {
      sobj->push_pod_noncontiguous(list_header_64);
      sobj->push_pod_contiguous(length);
    }
    return;
  case qstype::INTEGER:
    if(length < 32) {
      sobj->push_pod_noncontiguous( static_cast<uint8_t>(integer_header_5 | static_cast<uint8_t>(length)) );
    } else if(length < 256) { 
      sobj->push_pod_noncontiguous(integer_header_8);
      sobj->push_pod_contiguous(static_cast<uint8_t>(length) );
    } else if(length < 65536) { 
      sobj->push_pod_noncontiguous(integer_header_16);
      sobj->push_pod_contiguous(static_cast<uint16_t>(length) );
    } else if(length < 4294967296) {
      sobj->push_pod_noncontiguous(integer_header_32);
      sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    } else {
      sobj->push_pod_noncontiguous(integer_header_64);
      sobj->push_pod_contiguous(static_cast<uint64_t>(length) );
    }
    return;
  case qstype::LOGICAL:
    if(length < 32) {
      sobj->push_pod_noncontiguous( static_cast<uint8_t>(logical_header_5 | static_cast<uint8_t>(length)) );
    } else if(length < 256) { 
      sobj->push_pod_noncontiguous(logical_header_8);
      sobj->push_pod_contiguous(static_cast<uint8_t>(length) );
    } else if(length < 65536) { 
      sobj->push_pod_noncontiguous(logical_header_16);
      sobj->push_pod_contiguous(static_cast<uint16_t>(length) );
    } else if(length < 4294967296) {
      sobj->push_pod_noncontiguous(logical_header_32);
      sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    } else {
      sobj->push_pod_noncontiguous(logical_header_64);
      sobj->push_pod_contiguous(length);
    }
    return;
  case qstype::RAW:
    if(length < 4294967296) {
      sobj->push_pod_noncontiguous(raw_header_32);
      sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    } else {
      sobj->push_pod_noncontiguous(raw_header_64);
      sobj->push_pod_contiguous(length);
    }
    return;
  case qstype::CHARACTER:
    if(length < 32) {
      sobj->push_pod_noncontiguous( static_cast<uint8_t>(character_header_5 | static_cast<uint8_t>(length)) );
    } else if(length < 256) { 
      sobj->push_pod_noncontiguous(character_header_8);
      sobj->push_pod_contiguous(static_cast<uint8_t>(length) );
    } else if(length < 65536) { 
      sobj->push_pod_noncontiguous(character_header_16);
      sobj->push_pod_contiguous(static_cast<uint16_t>(length) );
    } else if(length < 4294967296) {
      sobj->push_pod_noncontiguous(character_header_32);
      sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    } else {
      sobj->push_pod_noncontiguous(character_header_64);
      sobj->push_pod_contiguous(static_cast<uint64_t>(length) );
    }
    return;
  case qstype::COMPLEX:
    if(length < 4294967296) {
      sobj->push_pod_noncontiguous(complex_header_32);
      sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    } else {
      sobj->push_pod_noncontiguous(complex_header_64);
      sobj->push_pod_contiguous(length);
    }
    return;
  case qstype::LOCKED_ENV:
    // sobj->push_pod_noncontiguous(extension_header, locked_env_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(locked_env_header);
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) ); // not really a length, but an index to hash position, allowing a reference
    return;
  case qstype::UNLOCKED_ENV:
    // sobj->push_pod_noncontiguous(extension_header, unlocked_env_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(unlocked_env_header);
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) ); // not really a length, but an index to hash position, allowing a reference
    return;
  // case qstype::PACKAGE_ENV:
  //   sobj->push_pod_noncontiguous(package_env_header_with_ext_32), 2);
  //   sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
  //   return;
  // case qstype::GLOBAL_ENV:
  //   sobj->push_pod_noncontiguous(global_env_header_with_ext), 2);
  //   return;
  // case qstype::BASE_ENV:
  //   sobj->push_pod_noncontiguous(base_env_header_with_ext), 2);
  //   return;
  // case qstype::EMPTY_ENV:
  //   sobj->push_pod_noncontiguous(empty_env_header_with_ext), 2);
  //   return;
  case qstype::REFERENCE:
    // sobj->push_pod_noncontiguous(extension_header, reference_object_header);
    sobj->push_pod_noncontiguous(extension_header);
    sobj->push_pod_contiguous(reference_object_header);
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) ); // not really a length, but a pointer to the hash reference
    return;
  case qstype::RSERIALIZED:
    if(length < 4294967296) {
      sobj->push_pod_noncontiguous(nstype_header_32);
      sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
    } else {
      sobj->push_pod_noncontiguous(nstype_header_64);
      sobj->push_pod_contiguous(static_cast<uint64_t>(length) );
    }
    return;
  case qstype::NIL:
    sobj->push_pod_noncontiguous(null_header);
    return;
  default:
    throw std::runtime_error("something went wrong writing object header");  // should never reach here
  }
}

template <class T>
void writeS4Flag_common(T * sobj) {
  // sobj->push_pod_noncontiguous(extension_header, s4flag_header);
  sobj->push_pod_noncontiguous(extension_header);
  sobj->push_pod_contiguous(s4flag_header);
}

template <class T>
void writeAttributeHeader_common(uint64_t length, T * const sobj) {
  if(length < 32) {
    sobj->push_pod_noncontiguous( static_cast<uint8_t>(attribute_header_5 | static_cast<uint8_t>(length)) );
  } else if(length < 256) {
    sobj->push_pod_noncontiguous(attribute_header_8);
    sobj->push_pod_contiguous(static_cast<uint8_t>(length) );
  } else {
    sobj->push_pod_noncontiguous(attribute_header_32);
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
  }
}

template <class T>
void writeStringHeader_common(const uint64_t length, const cetype_t ce_enc, T * const sobj) {
  uint8_t enc;
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
    sobj->push_pod_noncontiguous( static_cast<uint8_t>(string_header_5 | enc | static_cast<uint8_t>(length)) );
  } else if(length < 256) {
    sobj->push_pod_noncontiguous( static_cast<uint8_t>(string_header_8 | static_cast<uint8_t>(enc)) );
    sobj->push_pod_contiguous(static_cast<uint8_t>(length) );
  } else if(length < 65536) {
    sobj->push_pod_noncontiguous( static_cast<uint8_t>(string_header_16 | static_cast<uint8_t>(enc)) );
    sobj->push_pod_contiguous(static_cast<uint16_t>(length) );
  } else {
    sobj->push_pod_noncontiguous( static_cast<uint8_t>(string_header_32 | static_cast<uint8_t>(enc)) );
    sobj->push_pod_contiguous(static_cast<uint32_t>(length) );
  }
}

void getAttributes(SEXP const x, std::vector<SEXP> & attrs, std::vector<SEXP> & anames) {
  SEXP alist = ATTRIB(x);
  while(alist != R_NilValue) {
    anames.push_back(PRINTNAME(TAG(alist)));
    attrs.push_back(CAR(alist));
    alist = CDR(alist);
  }
}

template <class T>
void writeAttributes(T * const sobj, const std::vector<SEXP> & attrs, const std::vector<SEXP> & anames) {
  for(uint64_t i=0; i<anames.size(); i++) {
    uint32_t alen = strlen(CHAR(anames[i]));
    writeStringHeader_common(alen, CE_NATIVE, sobj);
    sobj->push_contiguous(CHAR(anames[i]), alen);
    writeObject(sobj, attrs[i]);
  }
}

template <class T>
void writeObject(T * const sobj, SEXP x) {
  std::vector<SEXP> attrs; // attribute objects and names; r-serialized, env-references and NULLs don't have attributes, so process inline
  std::vector<SEXP> anames; // just declare attribute variables for convienence here
  auto xtype = TYPEOF(x);
  if(IS_S4_OBJECT(x)) writeS4Flag_common(sobj);
  switch(xtype) {
  case NILSXP:
    writeHeader_common(qstype::NIL, 0, sobj);
    return;
  case S4SXP: // S4SXP is really just a scaffold for attributes
    getAttributes(x, attrs, anames);
    if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
    writeHeader_common(qstype::S4, 0, sobj);
    writeAttributes(sobj, attrs, anames);
    return;
  case STRSXP:
  {
    getAttributes(x, attrs, anames);
    if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
    uint64_t dl = Rf_xlength(x);
    writeHeader_common(qstype::CHARACTER, dl, sobj);
    if(is_unmaterialized_sf_vector(x)) {
      auto & ref = sf_vec_data_ref(x);
      for(uint64_t i=0; i<dl; i++) {
        switch(ref[i].encoding) {
        case cetype_t_ext::CE_NA:
          sobj->push_pod_noncontiguous(string_header_NA); // header is only 1 byte, but use noncontiguous for consistency
          break;
        case cetype_t_ext::CE_ASCII:
          writeStringHeader_common(ref[i].sdata.size(), CE_NATIVE, sobj);
          sobj->push_contiguous(ref[i].sdata.c_str(), ref[i].sdata.size());
          break;
        default:
          writeStringHeader_common(ref[i].sdata.size(), static_cast<cetype_t>(ref[i].encoding), sobj);
          sobj->push_contiguous(ref[i].sdata.c_str(), ref[i].sdata.size());
          break;
        }
      }
    } else {
      for(uint64_t i=0; i<dl; i++) {
        SEXP xi = STRING_ELT(x, i);
        if(xi == NA_STRING) {
          sobj->push_pod_noncontiguous(string_header_NA); // header is only 1 byte, but use noncontiguous for consistency
        } else {
          uint32_t di = LENGTH(xi);
          writeStringHeader_common(di, Rf_getCharCE(xi), sobj);
          sobj->push_contiguous(CHAR(xi), di);
        }
      }
    }

    writeAttributes(sobj, attrs, anames);
    return;
  }
  case SYMSXP:
  {
    if(x == R_MissingArg || x == R_UnboundValue || TYPEOF(PRINTNAME(x)) != CHARSXP) { // some special cases to handle1
      Protect_Tracker pt = Protect_Tracker();
      SEXP xserialized = PROTECT(serializeToRaw(x)); pt++;
      uint64_t xs_size = Rf_xlength(xserialized);
      writeHeader_common(qstype::RSERIALIZED, xs_size, sobj);
      sobj->push_contiguous(reinterpret_cast<char*>(RAW(xserialized)), xs_size);
      return;
    } else {
      SEXP a = PRINTNAME(x);
      getAttributes(x, attrs, anames);
      if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
      writeHeader_common(qstype::SYM, 0, sobj);
      uint32_t alen = strlen(CHAR(a));
      writeStringHeader_common(alen, Rf_getCharCE(a), sobj);
      sobj->push_contiguous(CHAR(a), alen);
      writeAttributes(sobj, attrs, anames);
      return;
    }
  }
  case VECSXP:
  {
    getAttributes(x, attrs, anames);
    if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
    uint64_t dl = Rf_xlength(x);
    writeHeader_common(qstype::LIST, dl, sobj);
    for(uint64_t i=0; i<dl; i++) {
      writeObject(sobj, VECTOR_ELT(x, i));
    }
    writeAttributes(sobj, attrs, anames);
    return;
  }
  case LISTSXP:
  {
    getAttributes(x, attrs, anames);
    if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
    std::vector<SEXP> cars;
    std::vector<SEXP> tags;
    std::vector<int> flags;
    SEXP xt = x;
    bool has_flags = false;
    while(xt != R_NilValue) {
      int f = packFlags(xt);
      if(f != 0) has_flags = true;
      flags.push_back(f);
      cars.push_back(CAR(xt));
      tags.push_back(TAG(xt));
      xt = CDR(xt);
    }
    if(has_flags) {
      // int flags = packFlags(x);
      // auto arr = pack_pods( flags, static_cast<uint32_t>(cars.size()) );
      // uint64_t mlen = unaligned_cast<uint64_t>(arr.data(), 0);
      writeHeader_common(qstype::PAIRLIST_WF, cars.size(), sobj);
      for(uint64_t i=0; i<cars.size(); i++) {
        sobj->push_pod_noncontiguous(flags[i]);
        if(tags[i] == R_NilValue) {
          sobj->push_pod_noncontiguous(string_header_NA);
        } else {
          const char * tag_chars = (CHAR(PRINTNAME(tags[i])));
          uint32_t alen = strlen(tag_chars);
          writeStringHeader_common(alen, CE_NATIVE, sobj);
          sobj->push_contiguous(tag_chars, alen);
        }
        writeObject(sobj, cars[i]);
      }
    } else {
      writeHeader_common(qstype::PAIRLIST, cars.size(), sobj);
      for(uint64_t i=0; i<cars.size(); i++) {
        if(tags[i] == R_NilValue) {
          sobj->push_pod_noncontiguous(string_header_NA);
        } else {
          const char * tag_chars = (CHAR(PRINTNAME(tags[i])));
          uint32_t alen = strlen(tag_chars);
          writeStringHeader_common(alen, CE_NATIVE, sobj);
          sobj->push_contiguous(tag_chars, alen);
        }
        writeObject(sobj, cars[i]);
      }
    }
    writeAttributes(sobj, attrs, anames);
    return;
  }
	case LANGSXP: // e.g. formulas
	case CLOSXP: // e.g. functions
	case PROMSXP:
	case DOTSXP:
    {
    getAttributes(x, attrs, anames);
    if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
    if(LEVELS(x) != 0 || OBJECT(x) != 0) {
      int flags = packFlags(x);
      switch(xtype) {
      case LANGSXP:
        writeHeader_common(qstype::LANG_WF, flags, sobj);
        break;
      case CLOSXP:
        writeHeader_common(qstype::CLOS_WF, flags, sobj);
        break;
      case PROMSXP:
        writeHeader_common(qstype::PROM_WF, flags, sobj);
        break;
      case DOTSXP:
        writeHeader_common(qstype::DOT_WF, flags, sobj);
        break;
      }
    } else {
      switch(xtype) {
      case LANGSXP:
        writeHeader_common(qstype::LANG, 0, sobj);
        break;
      case CLOSXP:
        writeHeader_common(qstype::CLOS, 0, sobj);
        break;
      case PROMSXP:
        writeHeader_common(qstype::PROM, 0, sobj);
        break;
      case DOTSXP:
        writeHeader_common(qstype::DOT, 0, sobj);
        break;
      }
    }
    /*
    if (BNDCELL_TAG(x)) { // may be necessary, see serialize.c in r source code
      R_expand_binding_value(x);
    }
    */
    writeObject(sobj, TAG(x));
    writeObject(sobj, CAR(x));
    writeObject(sobj, CDR(x)); // TAG/CAR/CDR are just accessors to elements; not real pairlist
    writeAttributes(sobj, attrs, anames);
    return;
  }
  case ENVSXP:
  {
    if(x == R_GlobalEnv || x == R_BaseEnv || x == R_EmptyEnv || 
                      R_IsNamespaceEnv(x) || R_IsPackageEnv(x)) {
      Protect_Tracker pt = Protect_Tracker();
      SEXP xserialized = PROTECT(serializeToRaw(x)); pt++;
      uint64_t xs_size = Rf_xlength(xserialized);
      writeHeader_common(qstype::RSERIALIZED, xs_size, sobj);
      sobj->push_contiguous(reinterpret_cast<char*>(RAW(xserialized)), xs_size);
    } else {
      if(sobj->object_ref_hash.map.find(x) != sobj->object_ref_hash.map.end()) {
        writeHeader_common(qstype::REFERENCE, sobj->object_ref_hash.map.at(x), sobj);
      } else {
        // std::cout << (void *)x << std::endl;
        sobj->object_ref_hash.add_to_hash(x);
        getAttributes(x, attrs, anames);
        if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
        if(R_EnvironmentIsLocked(x)) {
          writeHeader_common(qstype::LOCKED_ENV, sobj->object_ref_hash.index, sobj);
        } else {
          writeHeader_common(qstype::UNLOCKED_ENV, sobj->object_ref_hash.index, sobj);
        }
        writeObject(sobj, ENCLOS(x)); // parent env
        writeObject(sobj, FRAME(x));
        writeObject(sobj, HASHTAB(x));
        writeAttributes(sobj, attrs, anames);
      }
    }
    return;
  }
  case REALSXP:
  {
    getAttributes(x, attrs, anames);
    if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
    uint64_t dl = Rf_xlength(x);
    writeHeader_common(qstype::NUMERIC, dl, sobj);
    if(sobj->qm.real_shuffle) {
      sobj->shuffle_push(reinterpret_cast<char*>(REAL(x)), dl*8, 8);
    } else {
      sobj->push_contiguous(reinterpret_cast<char*>(REAL(x)), dl*8); 
    }
    writeAttributes(sobj, attrs, anames);
    return;
  }
  case INTSXP:
  {
    getAttributes(x, attrs, anames);
    if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
    uint64_t dl = Rf_xlength(x);
    writeHeader_common(qstype::INTEGER, dl, sobj);
    if(sobj->qm.int_shuffle) {
      sobj->shuffle_push(reinterpret_cast<char*>(INTEGER(x)), dl*4, 4);
    } else {
      sobj->push_contiguous(reinterpret_cast<char*>(INTEGER(x)), dl*4); 
    }
    writeAttributes(sobj, attrs, anames);
    return;
  }
  case LGLSXP:
  {
    getAttributes(x, attrs, anames);
    if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
    uint64_t dl = Rf_xlength(x);
    writeHeader_common(qstype::LOGICAL, dl, sobj);
    if(sobj->qm.lgl_shuffle) {
      sobj->shuffle_push(reinterpret_cast<char*>(LOGICAL(x)), dl*4, 4);
    } else {
      sobj->push_contiguous(reinterpret_cast<char*>(LOGICAL(x)), dl*4); 
    }
    writeAttributes(sobj, attrs, anames);
    return;
  }
  case RAWSXP:
  {
    getAttributes(x, attrs, anames);
    if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
    uint64_t dl = Rf_xlength(x);
    writeHeader_common(qstype::RAW, dl, sobj);
    sobj->push_contiguous(reinterpret_cast<char*>(RAW(x)), dl); 
    writeAttributes(sobj, attrs, anames);
    return;
  }
  case CPLXSXP:
  {
    getAttributes(x, attrs, anames);
    if(attrs.size() > 0) writeAttributeHeader_common(attrs.size(), sobj);
    uint64_t dl = Rf_xlength(x);
    writeHeader_common(qstype::COMPLEX, dl, sobj);
    if(sobj->qm.cplx_shuffle) {
      sobj->shuffle_push(reinterpret_cast<char*>(COMPLEX(x)), dl*16, 8);
    } else {
      sobj->push_contiguous(reinterpret_cast<char*>(COMPLEX(x)), dl*16); 
    }
    writeAttributes(sobj, attrs, anames);
    return;
  }
  default:
  {
    Protect_Tracker pt = Protect_Tracker();
    SEXP xserialized = PROTECT(serializeToRaw(x)); pt++;
    uint64_t xs_size = Rf_xlength(xserialized);
    writeHeader_common(qstype::RSERIALIZED, xs_size, sobj);
    sobj->push_contiguous(reinterpret_cast<char*>(RAW(xserialized)), xs_size);
    return;
  }
  }
}

#endif