#' qsave
#' 
#' Saves (serializes) an object to disk.  
#' 
#' @param file the file name/path.
#' @param compress_level The compression level (-50 to 22).  Default -1.  Higher values tend to have a better compression ratio, while lower values/negative values tend to be quicker.  Values with good compression/speed tradeoffs seem to be -1, 1 and 4.  
#' @return The de-serialized object
#' @examples 
#' x1 <- data.frame(int = sample(1e3, replace=TRUE), 
#'                  num = rnorm(1e3), 
#'                  char = randomStrings(1e3), stringsAsFactors = FALSE)
#' qsave(x1, "mydata.qs")
#' @name qsave
NULL

#' qread
#' 
#' Reads a object in a file serialized to disk
#' @param file the file name/path
#' @return The de-serialized object
#' @examples 
#' x1 <- data.frame(int = sample(1e3, replace=TRUE), 
#'                  num = rnorm(1e3), 
#'                  char = randomStrings(1e3), stringsAsFactors = FALSE)
#' qsave(x1, "mydata.qs")
#' x2 <- qread("mydata.qs")
#' identical(x1, x2) # returns true
#' @name qread
NULL

#' qdump
#' 
#' Exports uncompressed serialization to a list of Raw Vectors.  For testing purposes.  
#' 
#' @param file the file name/path.
#' @return The uncompressed serialization
#' @name qdump
NULL


#' Set Blocksize
#' 
#' Changes the compression blocksize.  Default: 2^19 (512 Kb).  Mostly for testing purposes, don't change otherwise.  
#' If you change the blocksize, you'll need to set it every time you load an object.  
#' @param blocksize The blocksize in bytes
#' @name qs_set_blocksize
NULL

#' Show Warnings
#' 
#' Show warnings during compression.  
#' @param show_warnings A boolean showing a warning when attempting to serialize an unsupported object type.  Default: TRUE.  
#' @name qs_show_warnings
NULL


#' Zstd CompressBound
#' 
#' Returns the maximum compressed size of an object of length `x`
#' @param x An integer size
#' @return maximum compressed size
#' @name zstd_compressBound
NULL


#' Zstd compression
#' 
#' Compression of raw vector
#' @param x A Raw Vector
#' @param compress_level The compression level (-50 to 22)
#' @return The compressed data
#' @name zstd_compress_raw
NULL

#' Zstd decompression
#' 
#' Decompresses of raw vector
#' @param x A Raw Vector
#' @return The uncompressed data
#' @name zstd_decompress_raw
NULL


#' System Endianness
#' 
#' Tests system endianness.  Intel and AMD based systems are little endian, and so this function will likely return `FALSE`.  
#' The `qs` package is not capable of transferring data between systems of different endianness. 
#' @return `TRUE` if big endian, `FALSE` if little endian. 
#' @name is_big_endian
NULL

#' Generate random strings
#' 
#' A function for generating a character vector of random strings, for testing purposes.  
#' @param N The number of random strings to generate
#' @param string_size The number of characters in each string (default 50). 
#' @return A character vector of random alpha-numeric strings.    
#' @name randomStrings
NULL

