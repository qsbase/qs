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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# 
# You can contact the author at:
#   https://github.com/traversc/qs


#' qsave
#' 
#' Saves (serializes) an object to disk.  
#' @usage qsave(x, file, 
#' preset = "high", algorithm = "zstd", compress_level = 4L, 
#' shuffle_control = 15L, check_hash=TRUE, nthreads = 1)
#' @param x the object to serialize.
#' @param file the file name/path.
#' @param preset One of "fast", "high" (default), "high", "archive", "uncompressed" or "custom".  See details.  
#' @param algorithm Compression algorithm used: "lz4", "zstd", "lz4hc", "zstd_stream" or "uncompressed".
#' @param compress_level The compression level used (Default 4).  For lz4, this number must be > 1 (higher is less compressed).  For zstd, a number between -50 to 22 (higher is more compressed).  
#' @param shuffle_control An integer setting the use of byte shuffle compression.  A value between 0 and 15 (Default 15).  See details.  
#' @param check_hash Default TRUE, compute a hash which can be used to verify file integrity during serialization
#' @param nthreads Number of threads to use.  Default 1.  
#' @return The total number of bytes written to the file (returned invisibly)
#' @details 
#' This function serializes and compresses R objects using block compresion with the option of byte shuffling.  
#' There are lots of possible parameters.  This function exposes three parameters related to compression level and byte shuffling. 
#' 
#' `compress_level` - Higher values tend to have a better compression ratio, while lower values/negative values tend to be quicker.  
#' Due to the format of qs, there is very little benefit to compression levels > 5 or so.  
#' 
#' `shuffle_control` - This sets which numerical R object types are subject to byte shuffling.  
#' Generally speaking, the more ordered/sequential an object is (e.g., `1:1e7`), the larger the potential benefit of byte shuffling.  
#' It is not uncommon to have several orders magnitude benefit to compression ratio or compression speed.  The more random an object is (e.g., `rnorm(1e7)`), 
#' the less potential benefit there is, even negative benefit is possible.  Integer vectors almost always benefit from byte shuffling whereas the results for numeric vectors are mixed.  
#' To control block shuffling, add +1 to the parameter for logical vectors, +2 for integer vectors, +4 for numeric vectors and/or +8 for complex vectors. 
#' 
#' The `preset` parameter has several different combination of parameter sets that are performant over a large variety of data.  
#' The `algorithm` parameter, `compression_level` and `shuffle_control` 
#' parameters are ignored unless `preset` is "custom".  "fast" preset: algorithm lz4, compress_level 100, shuffle_control 0.  
#' "balanced" preset: algorithm lz4, compress_level 1, shuffle_control 15.  
#' "high" preset: algorithm zstd, compress_level 4, shuffle_control 15.  
#' "archive" preset: algorithm zstd_stream, compress_level 14, shuffle_control 15. (zstd_stream is currently single threaded only) 
#' @examples 
#' x <- data.frame(int = sample(1e3, replace=TRUE), 
#'                  num = rnorm(1e3), 
#'                  char = randomStrings(1e3), stringsAsFactors = FALSE)
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
#' @export
qsave <- function(x, file, preset="high", algorithm="zstd", compress_level=4L, shuffle_control=15L, check_hash = TRUE, nthreads=1) {
  invisible(c_qsave(x, file, preset, algorithm, compress_level, shuffle_control, check_hash, nthreads))
}

#' qread
#' 
#' Reads an object in a file serialized to disk
  #' @usage qread(file, use_alt_rep=FALSE, strict=FALSE, nthreads=1)
#' @param file the file name/path
#' @param use_alt_rep Use alt rep when reading in string data.  Default: FALSE.  (Note: on R versions earlier than 3.5.0, this parameter does nothing.) 
#' @param strict Whether to throw an error or just report a warning (Default: FALSE, report warning)
#' @param nthreads Number of threads to use.  Default 1.  
#' @return The de-serialized object
#' @examples 
#' x <- data.frame(int = sample(1e3, replace=TRUE), 
#'                  num = rnorm(1e3), 
#'                  char = randomStrings(1e3), stringsAsFactors = FALSE)
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
#' @export
qread <- function(file, use_alt_rep=FALSE, strict=FALSE, nthreads=1) {
  c_qread(file, use_alt_rep, strict, nthreads)
}

#' qsave_fd
#' 
#' Saves an object to a file descriptor
#' @usage qsave_fd(x, fd, 
#' preset = "high", algorithm = "zstd", compress_level = 4L, 
#' shuffle_control = 15L, check_hash=TRUE)
#' @param x the object to serialize.
#' @param fd A file descriptor
#' @param preset One of "fast", "balanced" , "high" (default), "archive", "uncompressed" or "custom".  See details.  
#' @param algorithm Compression algorithm used: "lz4", "zstd", "lz4hc", "zstd_stream" or "uncompressed".
#' @param compress_level The compression level used (Default 4).  For lz4, this number must be > 1 (higher is less compressed).  For zstd, a number between -50 to 22 (higher is more compressed).  
#' @param shuffle_control An integer setting the use of byte shuffle compression.  A value between 0 and 15 (Default 15).  See details.  
#' @param check_hash Default TRUE, compute a hash which can be used to verify file integrity during serialization
#' @return the number of bytes serialized (returned invisibly)
#' @details 
#' This function serializes and compresses an R object to a stream using a file descriptor
#' If your data is important, make sure you know what happens on the other side of the pipe.  See examples for usage.   
#' @export
qsave_fd <- function(x, fd, preset = "high",  algorithm="zstd", compress_level=4L, shuffle_control=15L, check_hash = TRUE) {
  invisible(c_qsave_fd(x, fd, preset, algorithm, compress_level, shuffle_control, check_hash))
}


#' qread_fd
#' 
#' Reads an object from a file descriptor
#' @usage qread_fd(fd, use_alt_rep=FALSE, strict=FALSE)
#' @param fd A file descriptor
#' @param use_alt_rep Use alt rep when reading in string data.  Default: FALSE.  (Note: on R versions earlier than 3.5.0, this parameter does nothing.) 
#' @param strict Whether to throw an error or just report a warning (Default: FALSE, report warning)
#' @return The de-serialized object
#' @details
#' See `?qsave_fd` for additional details and examples.  
#' @export
qread_fd <- function(fd, use_alt_rep=FALSE, strict=FALSE) {
  c_qread_fd(fd, use_alt_rep, strict)
}

#' qsave_handle
#' 
#' Saves an object to a windows handle
#' @usage qsave_handle(x, handle, 
#' preset = "high", algorithm = "zstd", compress_level = 4L, 
#' shuffle_control = 15L, check_hash=TRUE)
#' @param x the object to serialize.
#' @param handle A windows handle external pointer
#' @param preset One of "fast", "balanced" , "high" (default), "archive", "uncompressed" or "custom".  See details.  
#' @param algorithm Compression algorithm used: "lz4", "zstd", "lz4hc", "zstd_stream" or "uncompressed".
#' @param compress_level The compression level used (Default 4).  For lz4, this number must be > 1 (higher is less compressed).  For zstd, a number between -50 to 22 (higher is more compressed).  
#' @param shuffle_control An integer setting the use of byte shuffle compression.  A value between 0 and 15 (Default 15).  See details.  
#' @param check_hash Default TRUE, compute a hash which can be used to verify file integrity during serialization
#' @return the number of bytes serialized (returned invisibly)
#' @details 
#' This function serializes and compresses an R object to a stream using a file descriptor
#' If your data is important, make sure you know what happens on the other side of the pipe.  See examples for usage.   
#' @export
qsave_handle <- function(x, handle, preset = "high",  algorithm="zstd", compress_level=4L, shuffle_control=15L, check_hash = TRUE) {
  invisible(c_qsave_handle(x, handle, preset, algorithm, compress_level, shuffle_control, check_hash))
}


#' qread_handle
#' 
#' Reads an object from a windows handle
#' @usage qread_handle(handle, use_alt_rep=FALSE, strict=FALSE)
#' @param handle A windows handle external pointer
#' @param use_alt_rep Use alt rep when reading in string data.  Default: FALSE.  (Note: on R versions earlier than 3.5.0, this parameter does nothing.) 
#' @param strict Whether to throw an error or just report a warning (Default: FALSE, report warning)
#' @return The de-serialized object
#' @details
#' See `?qsave_handle` for additional details and examples.  
#' @export
qread_handle <- function(handle, use_alt_rep=FALSE, strict=FALSE) {
  c_qread_handle(handle, use_alt_rep, strict)
}


#' qserialize
#' 
#' Saves an object to a raw vector 
#' @usage qserialize(x, preset = "high", 
#' algorithm = "zstd", compress_level = 4L, 
#' shuffle_control = 15L, check_hash=TRUE)
#' @param x the object to serialize.
#' @param preset One of "fast", "balanced", "high" (default), "archive", "uncompressed" or "custom".  See details.  
#' @param algorithm Compression algorithm used: "lz4", "zstd", "lz4hc", "zstd_stream" or "uncompressed".
#' @param compress_level The compression level used (Default 4).  For lz4, this number must be > 1 (higher is less compressed).  For zstd, a number between -50 to 22 (higher is more compressed).  
#' @param shuffle_control An integer setting the use of byte shuffle compression.  A value between 0 and 15 (Default 15).  See details.  
#' @param check_hash Default TRUE, compute a hash which can be used to verify file integrity during serialization
#' @details 
#' This function serializes and compresses an R object to a raw vctor
#' If your data is important, make sure you know what happens on the other side of the pipe.  See examples for usage.   
#' @export
qserialize <- function(x, preset = "high",  algorithm="zstd", compress_level=4L, shuffle_control=15L, check_hash = TRUE) {
  c_qserialize(x, preset, algorithm, compress_level, shuffle_control, check_hash)
}


#' qdeserialize
#' 
#' Reads an object from a fd
#' @usage qdeserialize(x, use_alt_rep=FALSE, strict=FALSE)
#' @param x a raw vector
#' @param use_alt_rep Use alt rep when reading in string data.  Default: FALSE.  (Note: on R versions earlier than 3.5.0, this parameter does nothing.) 
#' @param strict Whether to throw an error or just report a warning (Default: FALSE, report warning)
#' @return The de-serialized object
#' @details
#' See `?qeserialize` for additional details and examples.  
#' @export
qdeserialize <- function(x, use_alt_rep=FALSE, strict=FALSE) {
  c_qdeserialize(x, use_alt_rep, strict)
}

#' qread_ptr
#' 
#' Reads an object from a external pointer
#' @usage qread_ptr(pointer, length, use_alt_rep=FALSE, strict=FALSE)
#' @param pointer An external pointer to memory
#' @param length the length of the object in memory
#' @param use_alt_rep Use alt rep when reading in string data.  Default: FALSE.  (Note: on R versions earlier than 3.5.0, this parameter does nothing.) 
#' @param strict Whether to throw an error or just report a warning (Default: FALSE, report warning)
#' @return The de-serialized object
#' @export
qread_ptr <- function(pointer, length, use_alt_rep=FALSE, strict=FALSE) {
  c_qread_ptr(pointer, length, use_alt_rep, strict)
}

#' qdump
#' 
#' Exports the uncompressed binary serialization to a list of Raw Vectors.  For testing purposes and exploratory purposes mainly.  
#' @usage qdump(file)
#' @param file the file name/path.
#' @return The uncompressed serialization
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE), 
#'                  num = rnorm(1e3), 
#'                  char = randomStrings(1e3), stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qsave(x, myfile)
#' x2 <- qdump(myfile)
#' @export
qdump <- function(file) {
  c_qdump(normalizePath(file, mustWork=FALSE))
}


#' Zstd compress bound
#' 
#' Exports the compress bound function from the zstd library.  Returns the maximum compressed size of an object of length `size`.  
#' @usage zstd_compress_bound(size)
#' @param size An integer size
#' @return maximum compressed size
#' @examples
#' zstd_compress_bound(100000)
#' zstd_compress_bound(1e9)
#' @name zstd_compress_bound
NULL

#' Zstd compression
#' 
#' Compression of raw vector.  Exports the main zstd compression function.  
#' @usage zstd_compress_raw(x, compress_level)
#' @param x A Raw Vector
#' @param compress_level The compression level (-50 to 22)
#' @return The compressed data
#' @examples
#' x <- 1:1e6
#' xserialized <- serialize(x, connection=NULL)
#' xcompressed <- zstd_compress_raw(xserialized, compress_level = 1)
#' xrecovered <- unserialize(zstd_decompress_raw(xcompressed))
#' @name zstd_compress_raw
NULL

#' Zstd decompression
#' 
#' Decompresses of raw vector
#' @usage zstd_decompress_raw(x)
#' @param x A Raw Vector
#' @return The uncompressed data
#' @examples
#' x <- 1:1e6
#' xserialized <- serialize(x, connection=NULL)
#' xcompressed <- zstd_compress_raw(xserialized, compress_level = 1)
#' xrecovered <- unserialize(zstd_decompress_raw(xcompressed))
#' @name zstd_decompress_raw
NULL

#' lz4 compress bound
#' 
#' Exports the compress bound function from the lz4 library.  Returns the maximum compressed size of an object of length `size`.  
#' @usage lz4_compress_bound(size)
#' @param size An integer size
#' @return maximum compressed size
#' @examples
#' lz4_compress_bound(100000)
#' #' lz4_compress_bound(1e9)
#' @name lz4_compress_bound
NULL

#' lz4 compression
#' 
#' Compression of raw vector.  Exports the main lz4 compression function.  
#' @usage lz4_compress_raw(x, compress_level)
#' @param x A Raw Vector
#' @param compress_level The compression level (> 1). 
#' @return The compressed data
#' @examples
#' x <- 1:1e6
#' xserialized <- serialize(x, connection=NULL)
#' xcompressed <- lz4_compress_raw(xserialized, compress_level = 1)
#' xrecovered <- unserialize(lz4_decompress_raw(xcompressed))
#' @name lz4_compress_raw
NULL

#' lz4 decompression
#' 
#' Decompresses of raw vector
#' @usage lz4_decompress_raw(x)
#' @param x A Raw Vector
#' @return The uncompressed data
#' @examples
#' x <- 1:1e6
#' xserialized <- serialize(x, connection=NULL)
#' xcompressed <- lz4_compress_raw(xserialized, compress_level = 1)
#' xrecovered <- unserialize(lz4_decompress_raw(xcompressed))
#' @name lz4_decompress_raw
NULL


#' System Endianness
#' 
#' Tests system endianness.  Intel and AMD based systems are little endian, and so this function will likely return `FALSE`.  
#' The `qs` package is not capable of transferring data between systems of different endianness.  This should not matter for the large majority of use cases. 
#' @usage is_big_endian()
#' @return `TRUE` if big endian, `FALSE` if little endian. 
#' @examples
#' is_big_endian() # returns FALSE on Intel/AMD systems
#' @name is_big_endian
NULL

#' Generate random strings
#' 
#' A function for generating a character vector of random strings, for testing purposes. 
#' @usage randomStrings(N, string_size)
#' @param N The number of random strings to generate
#' @param string_size The number of characters in each string (default 50). 
#' @return A character vector of random alpha-numeric strings. 
#' @examples
#' randomStrings(N=10, string_size=20) # returns 10 alphanumeric strings of length 20
#' randomStrings(N=100, string_size=200) # returns 100 alphanumeric strings of length 200
#' @name randomStrings
NULL

#' Convert character vector to alt-rep
#' 
#' A function for generating a alt-rep object from a character vector, for users to experiment with the alt-rep system. 
#' This function is not available in R versions earlier than 3.5.0.
#' @usage convertToAlt(x)
#' @param x The character vector
#' @return The character vector in alt-rep form
#' @examples
#' xalt <- convertToAlt(randomStrings(N=10, string_size=20))
#' xalt2 <- convertToAlt(c("a", "b", "c"))
#' @name convertToAlt
NULL


#' Shuffle a raw vector 
#' 
#' A function for shuffling a raw vector using BLOSC shuffle routines
#' @usage blosc_shuffle_raw(x, bytesofsize)
#' @param x The raw vector
#' @param bytesofsize Either 4 or 8
#' @return The shuffled vector
#' @examples
#' x <- serialize(1L:1000L, NULL)
#' xshuf <- blosc_shuffle_raw(x, 4)
#' xunshuf <- blosc_unshuffle_raw(xshuf, 4)
#' @name blosc_shuffle_raw
NULL


#' Un-shuffle a raw vector 
#' 
#' A function for un-shuffling a raw vector using BLOSC un-shuffle routines
#' @usage blosc_unshuffle_raw(x, bytesofsize)
#' @param x The raw vector
#' @param bytesofsize Either 4 or 8
#' @return The unshuffled vector
#' @examples
#' x <- serialize(1L:1000L, NULL)
#' xshuf <- blosc_shuffle_raw(x, 4)
#' xunshuf <- blosc_unshuffle_raw(xshuf, 4)
#' @name blosc_unshuffle_raw
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

#' catquo
#' 
#' Prints a string with single quotes on a new line
#' @usage catquo(...)
#' @param ... Arguments passed to `cat` function
catquo <- function(...) {
  cat("'", ..., "'\n", sep = "")
}

#' basE91 Encoding
#' 
#' Encodes binary data (a raw vector) as ascii text using basE91 encoding format
#' @usage base91_encode(rawdata)
#' @param rawdata A raw vector
#' @details 
#' basE91 (capital E for stylization) is a binary to ascii encoding format created by Joachim Henke in 2005. 
#' The encoding has a dictionary using 91 out of 94 printable ASCII characters; excludes - (dash), \ (backslash) and ' (single quote).
#' The overhead (extra bytes used relative to binary) is 22.97\% on average. In comparison, base 64 encoding has an overhead of 33.33\%.  
#' Because the dictionary includes double quotes, basE91 encoded data must be single quoted when stored as a string in R.  
#' @references http://base91.sourceforge.net/
#' @name base91_encode
NULL

#' basE91 Decoding
#' 
#' Decodes a basE91 encoded string back to binary
#' @usage base91_decode(encoded_string)
#' @param encoded_string A string
#' @name base91_decode
NULL

#' Z85 Encoding
#' 
#' Encodes binary data (a raw vector) as ascii text using Z85 encoding format
#' @usage base85_encode(rawdata)
#' @param rawdata A raw vector
#' @details 
#' Z85 is a binary to ascii encoding format created by Pieter Hintjens in 2010 and is part of the ZeroMQ RFC. 
#' The encoding has a dictionary using 85 out of 94 printable ASCII characters. 
#' There are other base 85 encoding schemes, including Ascii85, which is popularized and used by Adobe. 
#' Z85 is distinguished by its choice of dictionary, which is suitable for easier inclusion into source code for many programming languages. 
#' The dictionary excludes all quote marks and other control characters, and requires no special treatment in R and most other languages. 
#' Note: although the official specification restricts input length to multiples of four bytes, the implementation here works with any input length.  
#' The overhead (extra bytes used relative to binary) is 25\%. In comparison, base 64 encoding has an overhead of 33.33\%. 

#' @references https://rfc.zeromq.org/spec/32/
#' @name base85_encode
NULL

#' Z85 Decoding
#' 
#' Decodes a Z85 encoded string back to binary
#' @usage base85_decode(encoded_string)
#' @param encoded_string A string
#' @name base85_decode
NULL
