AC_INIT(qs, 0.22.1, traversc@gmail.com)
AC_PATH_PROG([PKGCONF],[pkg-config],[],[$PATH:/usr/local/bin:ext/bin:ext:/sw/bin:/opt/bin:/opt/local/bin])

########################################################
### Predefined compile strings for different cases

LIBS=""
INCLUDE_PATHS="-IZSTD -IZSTD/common -IZSTD/decompress -IZSTD/compress -ILZ4"
LZ4_ZSTD_COMPILE_STRING='LIBZSTD = ZSTD/common/debug.o ZSTD/common/entropy_common.o ZSTD/common/error_private.o ZSTD/common/fse_decompress.o ZSTD/common/pool.o ZSTD/common/threading.o ZSTD/common/xxhash.o ZSTD/common/zstd_common.o ZSTD/compress/fse_compress.o ZSTD/compress/hist.o ZSTD/compress/huf_compress.o ZSTD/compress/zstd_compress.o ZSTD/compress/zstd_double_fast.o ZSTD/compress/zstd_fast.o ZSTD/compress/zstd_lazy.o ZSTD/compress/zstd_ldm.o ZSTD/compress/zstdmt_compress.o ZSTD/compress/zstd_opt.o ZSTD/decompress/huf_decompress.o ZSTD/decompress/zstd_ddict.o ZSTD/decompress/zstd_decompress_block.o ZSTD/decompress/zstd_decompress.o ZSTD/dictBuilder/cover.o ZSTD/dictBuilder/divsufsort.o ZSTD/dictBuilder/fastcover.o ZSTD/dictBuilder/zdict.o
LIBLZ4  = LZ4/lz4.o LZ4/lz4hc.o
LIBEXPANDBINDINGVALUE  = expand_binding_value.o
$(SHLIB): libQSZSTD.a	libQSLZ4.a libEXPANDBINDINGVALUE.a

libQSZSTD.a: $(LIBZSTD)
	$(AR) rcs libQSZSTD.a $(LIBZSTD)
	
libQSLZ4.a: $(LIBLZ4)
	$(AR) rcs libQSLZ4.a $(LIBLZ4)
	
libEXPANDBINDINGVALUE.a: $(LIBEXPANDBINDINGVALUE)
	$(AR) rcs libEXPANDBINDINGVALUE.a $(LIBEXPANDBINDINGVALUE)

clean:
	rm -f $(SHLIB) $(OBJECTS) $(LIBZSTD) libQSZSTD.a $(LIBLZ4) libQSLZ4.a $(LIBEXPANDBINDINGVALUE) libEXPANDBINDINGVALUE.a'
	
ZSTD_COMPILE_STRING='LIBZSTD = ZSTD/common/debug.o ZSTD/common/entropy_common.o ZSTD/common/error_private.o ZSTD/common/fse_decompress.o ZSTD/common/pool.o ZSTD/common/threading.o ZSTD/common/xxhash.o ZSTD/common/zstd_common.o ZSTD/compress/fse_compress.o ZSTD/compress/hist.o ZSTD/compress/huf_compress.o ZSTD/compress/zstd_compress.o ZSTD/compress/zstd_double_fast.o ZSTD/compress/zstd_fast.o ZSTD/compress/zstd_lazy.o ZSTD/compress/zstd_ldm.o ZSTD/compress/zstdmt_compress.o ZSTD/compress/zstd_opt.o ZSTD/decompress/huf_decompress.o ZSTD/decompress/zstd_ddict.o ZSTD/decompress/zstd_decompress_block.o ZSTD/decompress/zstd_decompress.o ZSTD/dictBuilder/cover.o ZSTD/dictBuilder/divsufsort.o ZSTD/dictBuilder/fastcover.o ZSTD/dictBuilder/zdict.o
LIBEXPANDBINDINGVALUE  = expand_binding_value.o
$(SHLIB): libQSZSTD.a libEXPANDBINDINGVALUE.a

libQSZSTD.a: $(LIBZSTD)
	$(AR) rcs libQSZSTD.a $(LIBZSTD)

libEXPANDBINDINGVALUE.a: $(LIBEXPANDBINDINGVALUE)
	$(AR) rcs libEXPANDBINDINGVALUE.a $(LIBEXPANDBINDINGVALUE)

clean:
	rm -f $(SHLIB) $(OBJECTS) $(LIBZSTD) libQSZSTD.a $(LIBEXPANDBINDINGVALUE) libEXPANDBINDINGVALUE.a'
	
LZ4_COMPILE_STRING='LIBLZ4 = LZ4/lz4.o LZ4/lz4hc.o
LIBEXPANDBINDINGVALUE  = expand_binding_value.o
$(SHLIB): libQSLZ4.a libEXPANDBINDINGVALUE.a

libQSLZ4.a: $(LIBLZ4)
	$(AR) rcs libQSLZ4.a $(LIBLZ4)

libEXPANDBINDINGVALUE.a: $(LIBEXPANDBINDINGVALUE)
	$(AR) rcs libEXPANDBINDINGVALUE.a $(LIBEXPANDBINDINGVALUE)

clean:
	rm -f $(SHLIB) $(OBJECTS) $(LIBLZ4) libQSLZ4.a $(LIBEXPANDBINDINGVALUE) libEXPANDBINDINGVALUE.a'
	
BASELINE_COMPILE_STRING='LIBEXPANDBINDINGVALUE  = expand_binding_value.o
$(SHLIB): libEXPANDBINDINGVALUE.a

libEXPANDBINDINGVALUE.a: $(LIBEXPANDBINDINGVALUE)
	$(AR) rcs libEXPANDBINDINGVALUE.a $(LIBEXPANDBINDINGVALUE)

clean:
	rm -f $(SHLIB) $(OBJECTS) $(LIBEXPANDBINDINGVALUE) libEXPANDBINDINGVALUE.a'
	
	
########################################################
### Configure args

AC_ARG_WITH([zstd-force-compile],
            AC_HELP_STRING([--with-zstd-force-compile],
                           [Force compilation of bundled zstd source files]),
            [zstd_force_compile="true"])
            
AC_ARG_WITH([lz4-force-compile],
            AC_HELP_STRING([--with-lz4-force-compile],
                           [Force compilation of bundled lz4 source files]),
            [lz4_force_compile="true"])

AC_ARG_WITH([zstd-include],
            AC_HELP_STRING([--with-zstd-include=INCLUDE_PATH],
                           [the location of zstd header files]),
            [zstd_include_path=$withval])
            
AC_ARG_WITH([zstd-lib],
            AC_HELP_STRING([--with-zstd-lib=LIB_PATH],
                           [the location of zstd library files]),
            [zstd_lib_path=$withval])
            
AC_ARG_WITH([lz4-include],
            AC_HELP_STRING([--with-lz4-include=INCLUDE_PATH],
                           [the location of lz4 header files]),
            [lz4_include_path=$withval])
            
AC_ARG_WITH([lz4-lib],
            AC_HELP_STRING([--with-lz4-lib=LIB_PATH],
                           [the location of lz4 library files]),
            [lz4_lib_path=$withval])
            
AC_ARG_WITH([simd],
            AC_HELP_STRING([--with-simd],
                           [Manually select SIMD support (options: AVX2, SSE2)]),
            [with_simd=$withval])
       
  
# This doesn't seem to be very portable, so we won't use it  
# AX_CHECK_COMPILE_FLAG([-msse2], [SSE2_SUPPORTED=1], [SSE2_SUPPORTED=0])
# AX_CHECK_COMPILE_FLAG([-mavx2], [AVX2_SUPPORTED=1], [AVX2_SUPPORTED=0])
# AX_EXT()

########################################################
#### Version value function

getVersion()
{
VERSION_STRING=$1
MAJOR=`echo $VERSION_STRING | cut -d. -f1`
MINOR=`echo $VERSION_STRING | cut -d. -f2`
RELEASE=`echo $VERSION_STRING | cut -d. -f3`
echo $(($MAJOR*100000+$MINOR*100+$RELEASE))
}

########################################################
#### Compile ZSTD checks

if test xx$zstd_force_compile = "xxtrue"; then
  echo "Compiling zstd from source due to --with-zstd-force-compile"
  COMPILE_ZSTD="true"
elif test "xx$zstd_include_path" != "xx"; then
  echo "Using user-defined zstd install paths"
    LIBS="${LIBS} -L${zstd_lib_path}"
    INCLUDE_PATHS="${INCLUDE_PATHS} -I${zstd_include_path}"
    COMPILE_ZSTD="false"
elif test "xx$PKGCONF" != "xx"; then
  if "${PKGCONF}" --exists libzstd; then
    VERSION_STRING=`${PKGCONF} --modversion libzstd`
    VER=`getVersion ${VERSION_STRING}`
    if test "${VER}" -ge 100400; then
      echo "zstd ${VERSION_STRING} dynamic library detected -- skipping zstd compilation"
      zstd_lib_path=`"${PKGCONF}" --libs libzstd`
      zstd_include_path=`"${PKGCONF}" --cflags-only-I libzstd`
      LIBS="${LIBS} ${zstd_lib_path}"
      INCLUDE_PATHS="${INCLUDE_PATHS} ${zstd_include_path}"
      COMPILE_ZSTD="false"
    else
      echo "zstd ${VERSION_STRING} dynamic library detected but is lower than bundled version (1.4.0) -- compiling from source"
      COMPILE_ZSTD="true"
    fi
  else
    echo "zstd dynamic library not detected -- compiling from source"
    COMPILE_ZSTD="true"
  fi
else
  echo "pkg-confg not detected -- compiling zstd from source"
  COMPILE_ZSTD="true"
fi

if test xx$lz4_force_compile = "xxtrue"; then
  echo "Compiling lz4 from source due to --with-lz4-force-compile"
  COMPILE_LZ4="true"
elif test "xx$lz4_include_path" != "xx"; then
  echo "Using user-defined LZ4 install paths"
    LIBS="${LIBS} -L${lz4_lib_path}"
    INCLUDE_PATHS="${INCLUDE_PATHS} -I${lz4_include_path}"
    COMPILE_LZ4="false"
elif test "xx$PKGCONF" != "xx"; then
  if "${PKGCONF}" --exists liblz4; then
    VERSION_STRING=`${PKGCONF} --modversion liblz4`
    VER=`getVersion ${VERSION_STRING}`
    if test ${VER} -ge 100901; then
      echo "lz4 ${VERSION_STRING} dynamic library detected -- skipping lz4 compilation"
      lz4_lib_path=`"${PKGCONF}" --libs liblz4`
      lz4_include_path=`"${PKGCONF}" --cflags-only-I liblz4`
      LIBS="${LIBS} ${lz4_lib_path}"
      INCLUDE_PATHS="${INCLUDE_PATHS} ${lz4_include_path}"
      COMPILE_LZ4="false"
    else
      echo "lz4 ${VERSION_STRING} dynamic library detected but is lower than bundled version (1.9.1) -- compiling from source"
      COMPILE_LZ4="true"
    fi
  else
    echo "lz4 dynamic library not detected -- compiling from source"
    COMPILE_LZ4="true"
  fi
else
  echo "pkg-confg not detected -- compiling lz4 from source"
  COMPILE_LZ4="true"
fi

if test xx$COMPILE_ZSTD = "xxtrue"; then
  if test xx$COMPILE_LZ4 = "xxtrue"; then
    COMPILE_STRING=$LZ4_ZSTD_COMPILE_STRING
    LIBS="${LIBS} -lQSZSTD -lQSLZ4"
  else
    COMPILE_STRING=$ZSTD_COMPILE_STRING
    LIBS="${LIBS} -lQSZSTD"
  fi
elif test xx$COMPILE_LZ4 = "xxtrue"; then
  COMPILE_STRING=$LZ4_COMPILE_STRING
  LIBS="${LIBS} -lQSLZ4"
else
  COMPILE_STRING=$BASELINE_COMPILE_STRING
fi

if test xx$with_simd = "xxAVX2"; then
  echo "Using AVX2"
  INCLUDE_PATHS="$INCLUDE_PATHS -mavx2"
elif test xx$with_simd = "xxSSE2"; then
  echo "Using SSE2"
  INCLUDE_PATHS="$INCLUDE_PATHS -msse2"
fi

# else
#  if test xx$AVX2_SUPPORTED = "xx1"; then
#    echo "AVX2 support detected"
#    INCLUDE_PATHS="$INCLUDE_PATHS -mavx2"
#  elif test xx$SSE2_SUPPORTED = "xx1"; then
#    echo "SSE2 supported detected"
#    INCLUDE_PATHS="$INCLUDE_PATHS -msse2"
#  fi
# fi

echo $LIBS
echo $INCLUDE_PATHS
echo $COMPILE_STRING

AC_SUBST([COMPILE_STRING], $COMPILE_STRING)
AC_SUBST([INCLUDE_PATHS], $INCLUDE_PATHS)

AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT
