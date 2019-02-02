# qs - Quick Serialization of R Objects
# Copyright (C) 2019-prsent Travers Ching
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# 
# You can contact the author at:
#   https://github.com/traversc/qs



#' qsave
#' 
#' Saves (serializes) an object to disk.  
#' @usage qsave(x, file, compress_level)
#' @param x the object to serialize.
#' @param file the file name/path.
#' @param compress_level The compression level (-50 to 22).  Default -1.  Higher values tend to have a better compression ratio, while lower values/negative values tend to be quicker.  Values with good compression/speed tradeoffs seem to be -1, 1 and 4.  
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
#' @name qsave
NULL

#' qread
#' 
#' Reads a object in a file serialized to disk
#' @usage qread(file)
#' @param file the file name/path
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
#' @name qread
NULL

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
#' @name qdump
NULL


#' Use alt-rep
#' 
#' Changes whether qread uses the alt-rep system.  If you experience issues or run out of memory, set to FALSE.
#' @usage qs_use_alt_rep(s)
#' @param s A boolean to determine whether `qread` uses alt-rep.  Default: TRUE.  
#' @examples
#' myfile <- tempfile()
#' qs_use_alt_rep(FALSE)
#' x <- randomStrings(1e3)
#' qsave(x, myfile)
#' x2 <- qread(myfile) # qs will no longer use alt-rep strings to load in character vector data
#' identical(x, x2) # returns true
#' @name qs_use_alt_rep
NULL


#' Zstd CompressBound
#' 
#' Exports the compress bound function from the zstd library.  Returns the maximum compressed size of an object of length `size`.  
#' @usage zstd_compressBound(size)
#' @param size An integer size
#' @return maximum compressed size
#' #' @examples
#' zstd_compressBound(100000)
#' #' zstd_compressBound(1e9)
#' @name zstd_compressBound
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
#' @usage convertToAlt(x)
#' @param x The character vector
#' @return The character vector in alt-rep form
#' @examples
#' xalt <- convertToAlt(randomStrings(N=10, string_size=20))
#' xalt2 <- convertToAlt(c("a", "b", "c"))
#' @name convertToAlt
NULL


