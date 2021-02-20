AC_INIT(qs, 0.22.1, traversc@gmail.com)
AC_PATH_PROG([PKGCONF],[pkg-config],[],[$PATH:/usr/local/bin:ext/bin:ext:/sw/bin:/opt/bin:/opt/local/bin])

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
#### Compile ZSTD/LZ4/SIMD checks

ZSTD_INCLUDE_PATH=""
LZ4_INCLUDE_PATH=""
ZSTD_LIBS=""
LZ4_LIBS=""
ZSTD_SHLIB=""
LZ4_SHLIB=""
ZSTD_CLEAN=""
LZ4_CLEAN=""
SIMD_FLAG=""

if test xx$zstd_force_compile = "xxtrue"; then
  echo "Compiling zstd from source due to --with-zstd-force-compile"
  COMPILE_ZSTD="true"
elif test "xx$zstd_include_path" != "xx"; then
  echo "Using user-defined zstd install paths"
    ZSTD_LIBS="-L${zstd_lib_path}"
    ZSTD_INCLUDE_PATH="-I${zstd_include_path}"
    COMPILE_ZSTD="false"
elif test "xx$PKGCONF" != "xx"; then
  if "${PKGCONF}" --exists libzstd; then
    VERSION_STRING=`${PKGCONF} --modversion libzstd`
    VER=`getVersion ${VERSION_STRING}`
    if test "${VER}" -ge 100400; then
      echo "zstd ${VERSION_STRING} dynamic library detected -- skipping zstd compilation"
      ZSTD_LIBS=`"${PKGCONF}" --libs libzstd`
      ZSTD_INCLUDE_PATH=`"${PKGCONF}" --cflags-only-I libzstd`
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
  echo "pkg-config not detected -- compiling zstd from source"
  COMPILE_ZSTD="true"
fi

if test xx$COMPILE_ZSTD = "xxtrue"; then
  ZSTD_LIBS="${LIBS} -lQSZSTD"
  ZSTD_INCLUDE_PATH="-IZSTD -IZSTD/common -IZSTD/decompress -IZSTD/compress"
  ZSTD_SHLIB="libQSZSTD.a"
  ZSTD_CLEAN="\$(LIBZSTD) libQSZSTD.a"
fi

if test xx$lz4_force_compile = "xxtrue"; then
  echo "Compiling lz4 from source due to --with-lz4-force-compile"
  COMPILE_LZ4="true"
elif test "xx$lz4_include_path" != "xx"; then
  echo "Using user-defined LZ4 install paths"
    LZ4_LIBS="-L${lz4_lib_path}"
    LZ4_INCLUDE_PATH="-I${lz4_include_path}"
    COMPILE_LZ4="false"
elif test "xx$PKGCONF" != "xx"; then
  if "${PKGCONF}" --exists liblz4; then
    VERSION_STRING=`${PKGCONF} --modversion liblz4`
    VER=`getVersion ${VERSION_STRING}`
    if test ${VER} -ge 100901; then
      echo "lz4 ${VERSION_STRING} dynamic library detected -- skipping lz4 compilation"
      LZ4_LIBS=`"${PKGCONF}" --libs liblz4`
      LZ4_INCLUDE_PATH=`"${PKGCONF}" --cflags-only-I liblz4`
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

if test xx$COMPILE_LZ4 = "xxtrue"; then
  LZ4_LIBS="-lQSLZ4"
  LZ4_INCLUDE_PATH="-ILZ4"
  LZ4_SHLIB="libQSLZ4.a"
  LZ4_CLEAN="\$(LIBLZ4) libQSLZ4.a"
fi

if test xx$with_simd = "xxAVX2"; then
  echo "Using AVX2"
  SIMD_FLAG="-mavx2"
elif test xx$with_simd = "xxSSE2"; then
  echo "Using SSE2"
  SIMD_FLAG="-msse2"
fi

echo $ZSTD_INCLUDE_PATH
echo $LZ4_INCLUDE_PATH
echo $ZSTD_LIBS
echo $LZ4_LIBS
echo $ZSTD_SHLIB
echo $LZ4_SHLIB
echo $ZSTD_CLEAN
echo $LZ4_CLEAN
echo $SIMD_FLAG

AC_SUBST([ZSTD_INCLUDE_PATH], $ZSTD_INCLUDE_PATH)
AC_SUBST([LZ4_INCLUDE_PATH], $LZ4_INCLUDE_PATH)
AC_SUBST([ZSTD_LIBS], $ZSTD_LIBS)
AC_SUBST([LZ4_LIBS], $LZ4_LIBS)
AC_SUBST([ZSTD_SHLIB], $ZSTD_SHLIB)
AC_SUBST([LZ4_SHLIB], $LZ4_SHLIB)
AC_SUBST([ZSTD_CLEAN], $ZSTD_CLEAN)
AC_SUBST([LZ4_CLEAN], $LZ4_CLEAN)
AC_SUBST([SIMD_FLAG], $SIMD_FLAG)

AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT
