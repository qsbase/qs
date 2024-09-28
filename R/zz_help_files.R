# qs - Quick Serialization of R Objects
# Copyright (C) 2019-present Travers Ching
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# You can contact the author at:
#   https://github.com/qsbase/qs


# Since roxygen2 doesn't parse the `@usage` block, it doesn't know what the usage is, and hence it doesn't find any parameters to inherit, so we can't rely on
# `@inheritParams`, cf. https://github.com/r-lib/roxygen2/issues/836#issuecomment-513476356
#
# Instead we use `@eval` to minimize duplication, cf. https://roxygen2.r-lib.org/articles/rd.html#evaluating-arbitrary-code
shared_params_save <- function(incl_file = FALSE,
                               incl_handle = FALSE,
                               incl_fd = FALSE) {
  c('@param x The object to serialize.',
    '@param file The file name/path.'[incl_file],
    '@param handle A windows handle external pointer.'[incl_handle],
    '@param fd A file descriptor.'[incl_fd],
    '@param preset One of `"fast"`, `"balanced"`, `"high"` (default), `"archive"`, `"uncompressed"` or `"custom"`. See section *Presets* for details.',
    '@param algorithm **Ignored unless `preset = "custom"`.** Compression algorithm used: `"lz4"`, `"zstd"`, `"lz4hc"`, `"zstd_stream"` or `"uncompressed"`.',
    '@param compress_level **Ignored unless `preset = "custom"`.** The compression level used.',
      '',
      '',
      'For lz4, this number must be > 1 (higher is less compressed).',
      '',
      '',
      'For zstd, a number  between `-50` to `22` (higher is more compressed). Due to the format of qs, there is very little benefit to compression levels > 5 ',
      'or so.',
    '@param shuffle_control **Ignored unless `preset = "custom"`.** An integer setting the use of byte shuffle compression. A value between `0` and `15` ',
      '(default `15`). See section *Byte shuffling* for details.',
    '@param check_hash Default `TRUE`, compute a hash which can be used to verify file integrity during serialization.')
}

shared_params_read <- c(
  '@param use_alt_rep Use ALTREP when reading in string data (default `FALSE`). On R versions prior to 3.5.0, this parameter does nothing.',
  '@param strict Whether to throw an error or just report a warning (default: `FALSE`, i.e. report warning).'
)

#' qsave
#'
#' Saves (serializes) an object to disk.
#'
#' This function serializes and compresses R objects using block compression with the option of byte shuffling.
#'
#' # Presets
#'
#' There are lots of possible parameters. To simplify usage, there are four main presets that are performant over a large variety of data:
#'
#' - **`"fast"`** is a shortcut for `algorithm = "lz4"`, `compress_level = 100` and `shuffle_control = 0`.
#' - **`"balanced"`** is a shortcut for `algorithm = "lz4"`, `compress_level = 1` and `shuffle_control = 15`.
#' - **`"high"`** is a shortcut for `algorithm = "zstd"`, `compress_level = 4` and `shuffle_control = 15`.
#' - **`"archive"`** is a shortcut for `algorithm = "zstd_stream"`, `compress_level = 14` and `shuffle_control = 15`. (`zstd_stream` is currently
#'   single-threaded only)
#'
#' To gain more control over compression level and byte shuffling, set `preset = "custom"`, in which case the individual parameters `algorithm`,
#' `compress_level` and `shuffle_control` are actually regarded.
#'
#' # Byte shuffling
#'
#' The parameter `shuffle_control` defines which numerical R object types are subject to *byte shuffling*. Generally speaking, the more ordered/sequential an
#' object is (e.g., `1:1e7`), the larger the potential benefit of byte shuffling. It is not uncommon to improve compression ratio or compression speed by
#' several orders of magnitude. The more random an object is (e.g., `rnorm(1e7)`), the less potential benefit there is, even negative benefit is possible.
#' Integer vectors almost always benefit from byte shuffling, whereas the results for numeric vectors are mixed. To control block shuffling, add +1 to the
#' parameter for logical vectors, +2 for integer vectors, +4 for numeric vectors and/or +8 for complex vectors.
#'
#' @usage qsave(x, file,
#' preset = "high", algorithm = "zstd", compress_level = 4L,
#' shuffle_control = 15L, check_hash=TRUE, nthreads = 1)
#'
#' @eval shared_params_save(incl_file = TRUE)
#' @param nthreads Number of threads to use. Default `1`.
#'
#' @return The total number of bytes written to the file (returned invisibly).
#' @export
#' @name qsave
#'
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'          stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qsave(x, myfile)
#' x2 <- qread(myfile)
#' identical(x, x2) # returns true
#'
#' # qs support multithreading
#' qsave(x, myfile, nthreads=2)
#' x2 <- qread(myfile, nthreads=2)
#' identical(x, x2) # returns true
#'
#' # Other examples
#' z <- 1:1e7
#' myfile <- tempfile()
#' qsave(z, myfile)
#' z2 <- qread(myfile)
#' identical(z, z2) # returns true
#'
#' w <- as.list(rnorm(1e6))
#' myfile <- tempfile()
#' qsave(w, myfile)
#' w2 <- qread(myfile)
#' identical(w, w2) # returns true
NULL

#' qread
#'
#' Reads an object in a file serialized to disk.
#'
#' @usage qread(file, use_alt_rep=FALSE, strict=FALSE, nthreads=1)
#'
#' @param file The file name/path.
#' @eval shared_params_read
#' @param nthreads Number of threads to use. Default `1`.
#'
#' @return The de-serialized object.
#' @export
#' @name qread
#'
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qsave(x, myfile)
#' x2 <- qread(myfile)
#' identical(x, x2) # returns true
#'
#' # qs support multithreading
#' qsave(x, myfile, nthreads=2)
#' x2 <- qread(myfile, nthreads=2)
#' identical(x, x2) # returns true
#'
#' # Other examples
#' z <- 1:1e7
#' myfile <- tempfile()
#' qsave(z, myfile)
#' z2 <- qread(myfile)
#' identical(z, z2) # returns true
#'
#' w <- as.list(rnorm(1e6))
#' myfile <- tempfile()
#' qsave(w, myfile)
#' w2 <- qread(myfile)
#' identical(w, w2) # returns true
NULL

#' qattributes
#'
#' Reads the attributes of an object serialized to disk.
#'
#' Equivalent to:
#'
#' `attributes(qread(file))`
#'
#' But more efficient. Attributes are stored towards the end of the file.
#' This function will read through
#' the contents of the file (without de-serializing the object itself),
#' and then de-serializes the attributes only.
#'
#' Because it is necessary to read through the file, pulling out attributes could
#' take a long time if the file is large. However, it should be much faster than
#' de-serializing the entire object first.
#'
#' @usage qattributes(file, use_alt_rep=FALSE, strict=FALSE, nthreads=1)
#'
#' @inherit qread params
#'
#' @return the attributes fo the serialized object.
#'
#' @export
#'
#' @examples
#'
#' file <- tempfile()
#' qsave(mtcars, file)
#'
#' attr1 <- qattributes(file)
#' attr2 <- attributes(qread(file))
#'
#' print(attr1)
#' # $names
#' # [1] "IAU Name"      "Designation"   "Const." ...
#'
#' # $row.names
#' # [1] 1 2 3 4 5
#
#' # $class
#' # [1] "data.frame"
#'
#' identical(attr1, attr2) # TRUE
#'
qattributes <- function(file, use_alt_rep = FALSE, strict = FALSE, nthreads = 1L) {
  output <- .Call(`_qs_c_qattributes`, file, use_alt_rep, strict, nthreads)
  rownames_attr <- output[["row.names"]]
  # special case for how R stores compact rownames
  # see src/main/attrib.c
  if(is.integer(rownames_attr) && length(rownames_attr == 2L) && is.na(rownames_attr[1L])) {
    output[["row.names"]] <- seq(1L, abs(rownames_attr[2L]))
  }
  output
}

#' qsave_fd
#'
#' Saves an object to a file descriptor.
#'
#' @usage qsave_fd(x, fd,
#' preset = "high", algorithm = "zstd", compress_level = 4L,
#' shuffle_control = 15L, check_hash=TRUE)
#'
#' @eval shared_params_save(incl_fd = TRUE)
#'
#' @inherit qsave return details
#' @inheritSection qsave Presets
#' @inheritSection qsave Byte shuffling
#' @export
#' @name qsave_fd
NULL

#' qread_fd
#'
#' Reads an object from a file descriptor.
#'
#' See [qsave_fd()] for additional details and examples.
#'
#' @usage qread_fd(fd, use_alt_rep=FALSE, strict=FALSE)
#'
#' @param fd A file descriptor.
#' @eval shared_params_read
#'
#' @inherit qread return
#' @export
#' @name qread_fd
NULL

#' qsave_handle
#'
#' Saves an object to a windows handle.
#'
#' @usage qsave_handle(x, handle,
#' preset = "high", algorithm = "zstd", compress_level = 4L,
#' shuffle_control = 15L, check_hash=TRUE)
#'
#' @eval shared_params_save(incl_handle = TRUE)
#'
#' @inherit qsave return details
#' @inheritSection qsave Presets
#' @inheritSection qsave Byte shuffling
#' @export
#' @name qsave_handle
NULL

#' qread_handle
#'
#' Reads an object from a windows handle.
#'
#' See [qsave_handle()] for additional details and examples.
#'
#' @usage qread_handle(handle, use_alt_rep=FALSE, strict=FALSE)
#'
#' @param handle A windows handle external pointer.
#' @eval shared_params_read
#'
#' @inherit qread return
#' @export
#' @name qread_handle
NULL

#' qserialize
#'
#' Saves an object to a raw vector.
#'
#' @usage qserialize(x, preset = "high",
#' algorithm = "zstd", compress_level = 4L,
#' shuffle_control = 15L, check_hash=TRUE)
#'
#' @eval shared_params_save()
#'
#' @return A raw vector.
#' @inherit qsave details
#' @inheritSection qsave Presets
#' @inheritSection qsave Byte shuffling
#' @export
#' @name qserialize
NULL

#' qdeserialize
#'
#' Reads an object from a raw vector.
#'
#' See [qserialize()] for additional details and examples.
#'
#' @usage qdeserialize(x, use_alt_rep=FALSE, strict=FALSE)
#'
#' @param x A raw vector.
#' @eval shared_params_read
#'
#' @inherit qread return
#' @export
#' @name qdeserialize
NULL

#' qread_ptr
#'
#' Reads an object from an external pointer.
#'
#' @usage qread_ptr(pointer, length, use_alt_rep=FALSE, strict=FALSE)
#'
#' @param pointer An external pointer to memory.
#' @param length The length of the object in memory.
#' @eval shared_params_read
#'
#' @inherit qread return
#' @export
#' @name qread_ptr
NULL

#' qdump
#'
#' Exports the uncompressed binary serialization to a list of raw vectors. For testing purposes and exploratory purposes mainly.
#'
#' @usage qdump(file)
#'
#' @param file A file name/path.
#'
#' @return The uncompressed serialization.
#' @export
#' @name qdump
#'
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qsave(x, myfile)
#' x2 <- qdump(myfile)
NULL

#' Zstd compress bound
#'
#' Exports the compress bound function from the zstd library. Returns the maximum compressed size of an object of length `size`.
#'
#' @usage zstd_compress_bound(size)
#'
#' @param size An integer size
#'
#' @return maximum compressed size
#' @export
#' @name zstd_compress_bound
#'
#' @examples
#' zstd_compress_bound(100000)
#' zstd_compress_bound(1e9)
NULL

#' Zstd compression
#'
#' Compresses to a raw vector using the zstd algorithm. Exports the main zstd compression function.
#'
#' @usage zstd_compress_raw(x, compress_level)
#'
#' @param x The object to serialize.
#' @param compress_level The compression level used (default `4`). A number between `-50` to `22` (higher is more compressed). Due to the format of qs, there is
#'   very little benefit to compression levels > 5 or so.
#'
#' @return The compressed data as a raw vector.
#' @export
#' @name zstd_compress_raw
#'
#' @examples
#' x <- 1:1e6
#' xserialized <- serialize(x, connection=NULL)
#' xcompressed <- zstd_compress_raw(xserialized, compress_level = 1)
#' xrecovered <- unserialize(zstd_decompress_raw(xcompressed))
NULL

#' Zstd decompression
#'
#' Decompresses a zstd compressed raw vector.
#'
#' @usage zstd_decompress_raw(x)
#'
#' @param x A raw vector.
#'
#' @inherit qread return
#' @export
#' @name zstd_decompress_raw
#'
#' @examples
#' x <- 1:1e6
#' xserialized <- serialize(x, connection=NULL)
#' xcompressed <- zstd_compress_raw(xserialized, compress_level = 1)
#' xrecovered <- unserialize(zstd_decompress_raw(xcompressed))
NULL

#' lz4 compress bound
#'
#' Exports the compress bound function from the lz4 library. Returns the maximum compressed size of an object of length `size`.
#'
#' @usage lz4_compress_bound(size)
#'
#' @param size An integer size.
#'
#' @return Maximum compressed size.
#' @export
#' @name lz4_compress_bound
#'
#' @examples
#' lz4_compress_bound(100000)
#' #' lz4_compress_bound(1e9)
NULL

#' lz4 compression
#'
#' Compresses to a raw vector using the lz4 algorithm. Exports the main lz4 compression function.
#'
#' @usage lz4_compress_raw(x, compress_level)
#'
#' @param x The object to serialize.
#' @param compress_level The compression level used. A number > 1 (higher is less compressed).
#'
#' @inherit zstd_compress_raw return
#' @export
#' @name lz4_compress_raw
#'
#' @examples
#' x <- 1:1e6
#' xserialized <- serialize(x, connection=NULL)
#' xcompressed <- lz4_compress_raw(xserialized, compress_level = 1)
#' xrecovered <- unserialize(lz4_decompress_raw(xcompressed))
NULL

#' lz4 decompression
#'
#' Decompresses an lz4 compressed raw vector.
#'
#' @usage lz4_decompress_raw(x)
#'
#' @param x A raw vector.
#'
#' @inherit qread return
#' @export
#' @name lz4_decompress_raw
#'
#' @examples
#' x <- 1:1e6
#' xserialized <- serialize(x, connection=NULL)
#' xcompressed <- lz4_compress_raw(xserialized, compress_level = 1)
#' xrecovered <- unserialize(lz4_decompress_raw(xcompressed))
NULL

#' System Endianness
#'
#' Tests system endianness. Intel and AMD based systems are little endian, and so this function will likely return `FALSE`.
#' The `qs` package is not capable of transferring data between systems of different endianness. This should not matter for the large majority of use cases.
#'
#' @usage is_big_endian()
#'
#' @return `TRUE` if big endian, `FALSE` if little endian.
#' @export
#' @name is_big_endian
#'
#' @examples
#' is_big_endian() # returns FALSE on Intel/AMD systems
NULL

#' Shuffle a raw vector
#'
#' Shuffles a raw vector using BLOSC shuffle routines.
#'
#' @usage blosc_shuffle_raw(x, bytesofsize)
#'
#' @param x A raw vector.
#' @param bytesofsize Either `4` or `8`.
#'
#' @return The shuffled vector
#' @export
#' @name blosc_shuffle_raw
#'
#' @examples
#' x <- serialize(1L:1000L, NULL)
#' xshuf <- blosc_shuffle_raw(x, 4)
#' xunshuf <- blosc_unshuffle_raw(xshuf, 4)
NULL

#' Un-shuffle a raw vector
#'
#' Un-shuffles a raw vector using BLOSC un-shuffle routines.
#'
#' @usage blosc_unshuffle_raw(x, bytesofsize)
#'
#' @param x A raw vector.
#' @param bytesofsize Either `4` or `8`.
#'
#' @return The unshuffled vector.
#' @export
#' @name blosc_unshuffle_raw
#'
#' @examples
#' x <- serialize(1L:1000L, NULL)
#' xshuf <- blosc_shuffle_raw(x, 4)
#' xunshuf <- blosc_unshuffle_raw(xshuf, 4)
NULL

#' Official list of IAU Star Names
#'
#' Data from the International Astronomical Union.
#' An official list of the 336 internationally recognized named stars,
#' updated as of June 1, 2018.
#'
#' @docType data
#'
#' @usage data(starnames)
#'
#' @format A `data.frame` with official IAU star names and several properties, such as coordinates.
#'
#' @keywords datasets
#'
#' @references E Mamajek et. al. (2018),
#' \emph{WG Triennial Report (2015-2018) - Star Names}, Reports on Astronomy, 22 Mar 2018.
#
#' @source \href{https://www.iau.org/public/themes/naming_stars/}{Naming Stars | International Astronomical Union.}
#'
#' @examples
#' data(starnames)
"starnames"


#' Register ALTREP class for serialization
#'
#' Register an ALTREP class to serialize using base R serialization. 
#'
#' @usage register_altrep_class(classname, pkgname)
#'
#' @param classname The ALTREP class name
#' @param pkgname The package the ALTREP class comes from
#'
#' @export
#' @name register_altrep_class
#'
#' @examples
#' register_altrep_class("compact_intseq", "base")
NULL

#' Unegister ALTREP class for serialization
#'
#' Unegister an ALTREP class to not use base R serialization. 
#'
#' @usage unregister_altrep_class(classname, pkgname)
#'
#' @param classname The ALTREP class name
#' @param pkgname The package the ALTREP class comes from
#'
#' @export
#' @name unregister_altrep_class
#'
#' @examples
#' unregister_altrep_class("compact_intseq", "base")
NULL

#' Get the class information of an ALTREP object
#'
#' Gets the formal name of the class and package of an ALTREP object
#'
#' @usage get_altrep_class_info(obj)
#'
#' @param obj The ALTREP class name
#' @return The class information (class name and package name) of an ALTREP object, a character vector of length two. 
#' If the object is not an ALTREP object, returns NULL. 
#' 
#' @export
#' @name get_altrep_class_info
#'
#' @examples
#' get_altrep_class_info(1:5)
NULL

#' Allow for serialization/deserialization of promises
#'
#' Allow for serialization/deserialization of promises
#'
#' @usage set_trust_promises(value)
#'
#' @param value a boolean `TRUE` or `FALSE`
#' @return The previous value of the global variable `trust_promises`
#' 
#' @export
#' @name set_trust_promises
#'
#' @examples
#' set_trust_promises(TRUE)
NULL

