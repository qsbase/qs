---
title: "README"
author: Travers Ching
---

  # qs [![Build Status](https://travis-ci.org/traversc/qs.svg)](https://travis-ci.org/traversc/qs)

Quick serialization of R objects

The goal of this package is to provide an interface for quickly writing (serializing) and reading (de-serializing) R objects to and from disk.  Inspired by `fst`, this package takes a more general approach for attributes and object references while providing comparable serialization speed.  This more general approach allows for serialization of lists, nested lists and attributes.  Hence, any S3 object built on common data types can also be serialized (e.g., `tibble`s, time-stamps, `bit64`, etc.)  

## Features
The table below compares the features of different serialization approaches in R.


|                    | qs         | fst           | saveRDS  |
|--------------------|:-----------:|:---------------:|:----------:|
| Not Slow             | &#10004;   | &#10004;       | &#10060; |
| Numeric Vectors    | &#10004;   | &#10004;       | &#10004;  |
| Integer Vectors    | &#10004;   | &#10004;       | &#10004;  |
| Logical Vectors    | &#10004;   | &#10004;       | &#10004;  |
| Character Vectors  | &#10004;   | &#10004;       | &#10004;  |
| Character Encoding | &#10004;   | (vector-wide only) | &#10004;  |
| Complex Vectors    | &#10004;   | &#10060;      | &#10004;  |
| Data.Frames        | &#10004;   | &#10004;       | &#10004;  |
| On disk row access | &#10060;  | &#10004;       | &#10060; |
| Attributes         | &#10004;   | Some          | &#10004;  |
| Lists / Nested Lists| &#10004;   |  &#10060;     | &#10004;  |
| Multi-threaded     | &#10060; (Not Yet) | &#10004;      |  &#10060;   |

## Benchmarks

### Data.Frame benchmark

Benchmarks for serializing and de-serializing large data.frames (5 million rows) composed of a numeric column (`rnorm`), an integer column (`sample(5e6)`), and a character vector column (random alphanumeric strings of length 50).  See `dataframe_bench.png` for a comparison using different compression parameters.  

#### Serialization speed with default parameters:
| Method         | write time (s) | read time (s) |
|----------------|----------------|---------------|
| qs             | 0.49391294     | 8.8818166     |
| fst (1 thread) | 0.37411811     | 8.9309314     |
| fst (4 thread) | 0.3676273      | 8.8565951     |
| saveRDS        | 14.377122      | 12.467517     |

#### Serialization speed with different parameters

The numbers in the figure reflect the compression parameter used.  `qs` uses the `zstd` compression library, and compression parameters range from -50 to 22 (`qs` uses a default value of -1).  `fst` defines it's own compression range through a combination of `zstd` and `lz4` algorithms, ranging from 0 to 100 (default: 0).  

<img src="https://raw.githubusercontent.com/traversc/qs/master/vignettes/dataframe_bench.png" width="576">

### Nested List benchmark
Benchmarks for serialization of random nested lists with random attributes (approximately 50 Mb).  See the nested list example in the tests folder.  

#### Serialization speed with default parameters:
| Method  | write time (s) | read time (s) |
|---------|----------------|---------------|
| qs      | 0.17840716     | 0.19489372    |
| saveRDS | 3.484225       | 0.58762548    |

<img src="https://raw.githubusercontent.com/traversc/qs/master/vignettes/nested_list_bench.png" width="576">
