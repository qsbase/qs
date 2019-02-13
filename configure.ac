AC_INIT(qs, 1.3, traversc@gmail.com)
AC_PATH_PROG([PKGCONF],[pkg-config],[],[$PATH:/usr/local/bin:ext/bin:ext:/sw/bin:/opt/bin:/opt/local/bin])

LIBS="-L. -lzstd"
INCLUDE_PATHS="-IZSTD/ -IZSTD/common -IZSTD/decompress -IZSTD/compress"
ZSTD_COMPILE_STRING='LIBZSTD = ZSTD/common/debug.o ZSTD/common/entropy_common.o ZSTD/common/error_private.o ZSTD/common/fse_decompress.o ZSTD/common/pool.o ZSTD/common/threading.o ZSTD/common/xxhash.o ZSTD/common/zstd_common.o ZSTD/compress/fse_compress.o ZSTD/compress/hist.o ZSTD/compress/huf_compress.o ZSTD/compress/zstd_compress.o ZSTD/compress/zstd_double_fast.o ZSTD/compress/zstd_fast.o ZSTD/compress/zstd_lazy.o ZSTD/compress/zstd_ldm.o ZSTD/compress/zstdmt_compress.o ZSTD/compress/zstd_opt.o ZSTD/decompress/huf_decompress.o ZSTD/decompress/zstd_ddict.o ZSTD/decompress/zstd_decompress_block.o ZSTD/decompress/zstd_decompress.o ZSTD/dictBuilder/cover.o ZSTD/dictBuilder/divsufsort.o ZSTD/dictBuilder/fastcover.o ZSTD/dictBuilder/zdict.o
$(SHLIB): libzstd.a	

libzstd.a: $(LIBZSTD)
	$(AR) rcs libzstd.a $(LIBZSTD)

clean:
	rm -f $(SHLIB) $(OBJECTS) $(LIBZSTD) libzstd.a'

AC_ARG_WITH([zstd-force-compile],
            AC_HELP_STRING([--with-zstd-force-compile],
                           [Force compilation of bundled zstd source files]),
            [force_compile="true"])

AC_ARG_WITH([zstd-include],
            AC_HELP_STRING([--with-zstd-include=INCLUDE_PATH],
                           [the location of zstd header files]),
            [zstd_include_path=$withval])
            
AC_ARG_WITH([zstd-lib],
            AC_HELP_STRING([--with-zstd-lib=LIB_PATH],
                           [the location of zstd library files]),
            [zstd_lib_path=$withval])

if test xx$force_compile = "xxtrue"; then
  echo "Compiling from source due to --with-zstd-force-compile"
elif test "xx$zstd_include_path" != "xx"; then
  echo "Using user-defined zstd install paths"
  LIBS="-L. -L${zstd_lib_path} -lzstd"
  ZSTD_COMPILE_STRING=""
  INCLUDE_PATHS=-I$zstd_include_path
elif test "xx$PKGCONF" != "xx"; then
  if "${PKGCONF}" --exists libzstd; then
    VERSION_STRING=`"${PKGCONF}" --modversion libzstd`
    VER_MAJOR_STRING=`echo $VERSION_STRING | cut -d. -f1`
    VER_MINOR_STRING=`echo $VERSION_STRING | cut -d. -f2`
    VER_RELEASE_STRING=`echo $VERSION_STRING | cut -d. -f3`
    echo $VER_MAJOR_STRING $VER_MINOR_STRING $VER_RELEASE_STRING
    IS_HIGHER_VERSION="false";
    if test "$VER_MAJOR_STRING" -gt 1; then
      IS_HIGHER_VERSION="true"
    elif test "$VER_MAJOR_STRING" -eq 1; then
      if test "$VER_MINOR_STRING" -ge 3; then
        IS_HIGHER_VERSION="true"
      fi
    fi
    if test xx$IS_HIGHER_VERSION = "xxtrue"; then
      echo "zstd ${VERSION_STRING} dynamic library detected -- skipping zstd compilation"
      LIBS=`"${PKGCONF}" --libs libzstd`
      INCLUDE_PATHS=`"${PKGCONF}" --cflags-only-I libzstd`
      ZSTD_COMPILE_STRING=""
    else
      echo "zstd ${VERSION_STRING} dynamic library detected but is lower than bundled version -- compiling from source"
    fi
  else
    echo "zstd dynamic library not detected -- compiling from source"
  fi
else
  echo "pkg-confg not detected -- compiling from source"
fi

echo $LIBS
echo $INCLUDE_PATHS
echo $ZSTD_COMPILE_STRING

AC_SUBST([ZSTD_COMPILE_STRING], $ZSTD_COMPILE_STRING)
AC_SUBST([INCLUDE_PATHS], $INCLUDE_PATHS)

AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT
