qs
================

<!-- <img src="qshex.png" width = "130" height = "150" align="right" style="border:0px;padding:15px"> -->

[![Build
Status](https://travis-ci.org/traversc/qs.svg)](https://travis-ci.org/traversc/qs)
[![CRAN\_Status\_Badge](http://www.r-pkg.org/badges/version/qs)](https://cran.r-project.org/package=qs)
[![CRAN\_Downloads\_Badge](https://cranlogs.r-pkg.org/badges/qs)](https://cran.r-project.org/package=qs)
[![CRAN\_Downloads\_Total\_Badge](https://cranlogs.r-pkg.org/badges/grand-total/qs)](https://cran.r-project.org/package=qs)

*Quick serialization of R objects*

`qs` provides an interface for quickly saving and reading objects to and
from disk. The goal of this package is to provide a lightning-fast and
complete replacement for the `saveRDS` and `readRDS` functions in R.

Inspired by the `fst` package, `qs` uses a similar block-compression
design using either the `lz4` or `zstd` compression libraries. It
differs in that it applies a more general approach for attributes and
object references.

`saveRDS` and `readRDS` are the standard for serialization of R data,
but these functions are not optimized for speed. On the other hand,
`fst` is extremely fast, but only works on `data.frame`’s and certain
column types.

`qs` is both extremely fast and general: it can serialize any R object
like `saveRDS` and is just as fast and sometimes faster than `fst`.

## Usage

``` r
library(qs)
df1 <- data.frame(x=rnorm(5e6), y=sample(5e6), z=sample(letters,5e6, replace=T))
qsave(df1, "myfile.qs")
df2 <- qread("myfile.qs")
```

## Installation:

``` r
# CRAN version
install.packages("qs")

# Linux/Mac CRAN version compile from source (recommended)
remotes::install_cran("qs", type="source", configure.args="--with-simd=AVX2 --with-allow-rconn")

# Windows CRAN version compile from source (recommended)
Sys.setenv(ALLOW_RCONN=1)
remotes::install_cran("qs", type="source")
```

## Features

The table below compares the features of different serialization
approaches in R.

|                      | qs |        fst         | saveRDS |
| -------------------- | :-: | :----------------: | :-----: |
| Not Slow             | ✔  |         ✔          |    ❌    |
| Numeric Vectors      | ✔  |         ✔          |    ✔    |
| Integer Vectors      | ✔  |         ✔          |    ✔    |
| Logical Vectors      | ✔  |         ✔          |    ✔    |
| Character Vectors    | ✔  |         ✔          |    ✔    |
| Character Encoding   | ✔  | (vector-wide only) |    ✔    |
| Complex Vectors      | ✔  |         ❌          |    ✔    |
| Data.Frames          | ✔  |         ✔          |    ✔    |
| On disk row access   | ❌  |         ✔          |    ❌    |
| Attributes           | ✔  |        Some        |    ✔    |
| Lists / Nested Lists | ✔  |         ❌          |    ✔    |
| Multi-threaded       | ✔  |         ✔          |    ❌    |

`qs` also includes a number of advanced features:

  - For character vectors, qs also has the option of using the new
    alt-rep system (R version 3.5+) to quickly read in string data.
  - For numerical data (numeric, integer, logical and complex vectors)
    `qs` implements byte shuffling filters (adopted from the Blosc
    meta-compression library). These filters utilize extended CPU
    instruction sets (either SSE2 or AVX2).

Both of these features have the possibility of additionally increasing
performance by orders of magnitude, for certain types of data. See
sections below for more details.

## Summary Benchmarks

The following benchmarks were performed on a Ryzen 2700x desktop using
various data types (detailed below). `qs` was compared with
`saveRDS`/`readRDS` in base R and the `fst` package for serializing and
de-serializing a medium sized `data.frame` with 5 million rows
(approximately 115 Mb):

``` r
data.frame(a=rnorm(5e6), 
           b=rpois(100,5e6),
           c=sample(starnames$IAU,5e6,T),
           d=sample(state.name,5e6,T),
           stringsAsFactors = F)
```

`qs` is highly parameterized and can be tuned by the user to extract as
much speed and compression as possible, if desired. For simplicity, `qs`
comes with 4 presets, which trades speed and compression ratio: “fast”,
“balanced”, “high” and “archive”.

The tables and plots below summarize the performance of `saveRDS`, `qs`
and `fst` with various
parameters:

### Summary table

| Algorithm                                | Threads | Write Time (s) | Read Time (s) | File Size (Mb) |
| :--------------------------------------- | ------: | -------------: | ------------: | -------------: |
| saveRDS / readRDS                        |       1 |          4.680 |         1.500 |           55.2 |
| saveRDS / readRDS                        |       4 |          1.370 |         1.050 |           55.0 |
| fst C=0                                  |       1 |          0.186 |         0.288 |          121.0 |
| fst C=0                                  |       4 |          0.184 |         0.286 |          121.0 |
| fst C=50                                 |       1 |          0.188 |         0.300 |           92.0 |
| fst C=50                                 |       4 |          0.183 |         0.296 |           92.0 |
| fst C=85                                 |       1 |          0.612 |         0.371 |           70.5 |
| fst C=85                                 |       4 |          0.463 |         0.332 |           70.5 |
| qs:lz4 shuffle=0 C=100 (fast)            |       1 |          0.196 |         0.319 |          106.0 |
| qs:lz4 shuffle=0 C=100                   |       4 |          0.161 |         0.322 |          106.0 |
| qs:lz4 shuffle=7 C=1 (balanced)          |       1 |          0.262 |         0.363 |           59.4 |
| qs:lz4 shuffle=7 C=1                     |       4 |          0.194 |         0.365 |           59.4 |
| qs:zstd shuffle=7 C=4 (high)             |       1 |          0.393 |         0.409 |           50.0 |
| qs:zstd shuffle=7 C=4                    |       4 |          0.212 |         0.411 |           50.0 |
| qs:zstd\_stream shuffle=7 C=14 (archive) |       1 |          9.160 |         0.452 |           46.9 |

### Serializing

![](vignettes/df_bench_write.png "df_bench_write")

### De-serializing

![](vignettes/df_bench_read.png "df_bench_read")

Benchmarking write and read speed is a bit tricky and depends highly on
a number of factors, such as operating system, the hardware being run
on, the distribution of the data, or even the state of the R instance.
Reading data is also further subjected to various hardware and software
memory caches.

Generally speaking, `qs` and `fst` are considerably faster than
`saveRDS` regardless of using single threaded or multi-threaded
compression. `qs` also manages to achieve superior compression ratio
through various optimizations (e.g. see “Byte Shuffle” section below).

## Byte Shuffle

Byte shuffling (adopted from the Blosc meta-compression library) is a
way of re-organizing data to be more ammenable to compression. For
example: an integer contains four bytes and the limits of an integer in
R are +/- 2^31-1. However, most real data doesn’t use anywhere near the
range of possible integer values. For example, if the data were
representing percentages, 0% to 100%, the first three bytes would be
unused and zero.

Byte shuffling rearranges the data such that all of the first bytes are
blocked together, the second bytes are blocked together, etc. This
procedure often makes it very easy for compression algorithms to find
repeated patterns and can often improves compression ratio by orders of
magnitude. In the example below, shuffle compression achieves a
compression ratio of over 1000x. See `?qsave` for more details.

``` r
# With byte shuffling
x <- 1:1e8
qsave(x, "mydat.qs", preset="custom", shuffle_control=15, algorithm="zstd")
cat( "Compression Ratio: ", as.numeric(object.size(x)) / file.info("mydat.qs")$size, "\n" )
# Compression Ratio:  1389.164

# Without byte shuffling
x <- 1:1e8
qsave(x, "mydat.qs", preset="custom", shuffle_control=0, algorithm="zstd")
cat( "Compression Ratio: ", as.numeric(object.size(x)) / file.info("mydat.qs")$size, "\n" )
# Compression Ratio:  1.479294 
```

## Alt-rep character vectors

The alt-rep system was introduced in R version 3.5. Briefly, alt-rep
vectors are objects that are not represented by R internal data, but
have accesor functions which promise to “materialize” elements within
the vector on the fly. To the user, this system is completely hidden and
appears seamless.

In `qs`, only alt-rep character vectors are implemented because it is
often the mostly costly of data types to read into R. Numeric and
integer data are already fast enough and do not largely benefit. An
example use case: if you have a large `data.frame`, and you are only
interested in processing certain columns, it is wasted computation to
materialize the whole `data.frame`. The alt-rep system solves this
problem.

``` r
df1 <- data.frame(x = randomStrings(1e6), y = randomStrings(1e6), stringsAsFactors = F)
qsave(df1, "temp.qs")
rm(df1); gc() ## remove df1 and call gc for proper benchmarking

# With alt-rep
system.time(qread("temp.qs", use_alt_rep=T))[1]
#     0.109 seconds


# Without alt-rep
gc(verbose=F)
system.time(qread("temp.qs", use_alt_rep=F))[1]
#     1.703 seconds
```

## Future developments

  - Additional compression algorithms
  - Non-blocked compressed options (for greater compression ratio)
  - Parameter optimization

Future versions will be backwards compatible with the current version.
