if(F) {
  trqwe::install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.12.tar.gz", "qs12") # zstd block compress only
  trqwe::install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.13.1.tar.gz", "qs131") # zstd block compress only
  trqwe::install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.14.1.tar.gz", "qs141") # zstd and lz4 block compress, byte shuffling
  trqwe::install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.15.1.tar.gz", "qs151") # zstd, lz4, lz4hc block compress
  trqwe::install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.16.1.tar.gz", "qs161") # zstd, lz4, lz4hc block compress, zstd_stream compress
  trqwe::install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.17.3.tar.gz", "qs173") # zstd, lz4, lz4hc block compress, zstd_stream compress
  trqwe::install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.18.3.tar.gz", "qs183") # zstd, lz4, lz4hc block compress, zstd_stream compress
  trqwe::install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.19.1.tar.gz", "qs191") # zstd, lz4, lz4hc block compress, zstd_stream compress
  trqwe::install_as_name("https://cran.r-project.org/src/contrib/qs_0.20.2.tar.gz", "qs202") # zstd, lz4, lz4hc block compress, zstd_stream compress
  
  
  # Earlier version cannot read zstd_stream from 0.17.1+ due to additional checksum at end of file
  # qs 0.18.1 -- header version 2 -- will not be readable by earlier versions
  
  # test if pacakges can be loaded
  library(qs12)
  library(qs131)
  library(qs141)
  library(qs151)
  library(qs161)
  library(qs173)
  library(qs183)
  library(qs191)
  library(qs202)
  library(qs)
}

file <- "/tmp/test.z"

dataframeGen <- function() {
  nr <- 1e6
  data.frame(a=rnorm(nr), 
             b=rpois(100,nr),
             c=sample(qs::starnames[["IAU Name"]],nr,T), 
             d=factor(sample(state.name,nr,T)), stringsAsFactors = F)
}

listGen <- function() {
  as.list(sample(1e6))
}

test_compatability <- function(save, read_funs) {
  x <- dataframeGen()
  save(x)
  for(i in 1:length(read_funs)) {
    cat(i)
    xu <- read_funs[[i]](file)
    stopifnot(identical(x, xu))
  }
  x <- listGen()
  save(x)
  for(i in 1:length(read_funs)) {
    cat(i)
    xu <- read_funs[[i]](file)
    stopifnot(identical(x, xu))
  }
  cat("\n")
}

qs12_save <- function(x) qs12::qsave(x, file)
qs131_save <- function(x) qs131::qsave(x, file)
qs141_lz4_save <- function(x) qs141::qsave(x, file, preset = "custom", algorithm = "lz4")
qs141_zstd_save <- function(x) qs141::qsave(x, file, preset = "custom", algorithm = "zstd")
qs151_lz4_save <- function(x) qs151::qsave(x, file, preset = "custom", algorithm = "lz4")
qs151_zstd_save <- function(x) qs151::qsave(x, file, preset = "custom", algorithm = "zstd")
qs161_lz4_save <- function(x) qs161::qsave(x, file, preset = "custom", algorithm = "lz4")
qs161_zstd_save <- function(x) qs161::qsave(x, file, preset = "custom", algorithm = "zstd")
qs161_zstd_stream_save <- function(x) qs161::qsave(x, file, preset = "custom", algorithm = "zstd_stream")
qs173_lz4_save <- function(x) qs173::qsave(x, file, preset = "custom", algorithm = "lz4")
qs173_zstd_save <- function(x) qs173::qsave(x, file, preset = "custom", algorithm = "zstd")
qs173_zstd_stream_save <- function(x) qs173::qsave(x, file, preset = "custom", algorithm = "zstd_stream")
qs173_zstd_stream_save_nohash <- function(x) qs173::qsave(x, file, preset = "custom", algorithm = "zstd_stream", check_hash = F)
qs173_no_shuffle <- function(x) qs173::qsave(x, file, preset = "custom", algorithm = "zstd", shuffle_control = 0)

qs183_lz4_save <- function(x) qs183::qsave(x, file, preset = "custom", algorithm = "lz4")
qs183_zstd_save <- function(x) qs183::qsave(x, file, preset = "custom", algorithm = "zstd")
qs183_zstd_stream_save <- function(x) qs183::qsave(x, file, preset = "custom", algorithm = "zstd_stream")
qs183_zstd_stream_save_nohash <- function(x) qs183::qsave(x, file, preset = "custom", algorithm = "zstd_stream", check_hash = F)
qs183_no_shuffle <- function(x) qs183::qsave(x, file, preset = "custom", algorithm = "zstd", shuffle_control = 0)

qs191_lz4_save <- function(x) qs191::qsave(x, file, preset = "custom", algorithm = "lz4")
qs191_zstd_save <- function(x) qs191::qsave(x, file, preset = "custom", algorithm = "zstd")
qs191_zstd_stream_save <- function(x) qs191::qsave(x, file, preset = "custom", algorithm = "zstd_stream")
qs191_zstd_stream_save_nohash <- function(x) qs191::qsave(x, file, preset = "custom", algorithm = "zstd_stream", check_hash = F)
qs191_no_shuffle <- function(x) qs191::qsave(x, file, preset = "custom", algorithm = "zstd", shuffle_control = 0)

qs202_lz4_save <- function(x) qs::qsave(x, file, preset = "custom", algorithm = "lz4")
qs202_zstd_save <- function(x) qs::qsave(x, file, preset = "custom", algorithm = "zstd")
qs202_zstd_stream_save <- function(x) qs::qsave(x, file, preset = "custom", algorithm = "zstd_stream")
qs202_zstd_stream_save_nohash <- function(x) qs::qsave(x, file, preset = "custom", algorithm = "zstd_stream", check_hash = F)
qs202_no_shuffle <- function(x) qs::qsave(x, file, preset = "custom", algorithm = "zstd", shuffle_control = 0)


if(F) {
  print("qs12 save"); test_compatability(qs12_save, list(qs12::qread, qs131::qread, qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs131 save"); test_compatability(qs131_save, list(qs12::qread, qs131::qread, qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs141 lz4 save"); test_compatability(qs141_lz4_save, list(qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs141 zstd save"); test_compatability(qs141_zstd_save, list(qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs151 lz4 save"); test_compatability(qs151_lz4_save, list(qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs151 zstd save"); test_compatability(qs151_zstd_save, list(qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs161 lz4 save"); test_compatability(qs161_lz4_save, list(qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs161 zstd save"); test_compatability(qs161_zstd_save, list(qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  
  print("qs161 zstd stream save"); test_compatability(qs161_zstd_stream_save, list(qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs173 lz4 save"); test_compatability(qs173_lz4_save, list(qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs173 zstd save"); test_compatability(qs173_zstd_save, list(qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs173 zstd stream save"); test_compatability(qs173_zstd_stream_save, list(qs183::qread, qs191::qread, qs::qread))
  print("qs173 zstd stream save no hash"); test_compatability(qs173_zstd_stream_save_nohash, list(qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  print("qs173 no shuffle save"); test_compatability(qs173_no_shuffle, list(qs12::qread, qs131::qread, qs141::qread, qs151::qread, qs161::qread, qs173::qread, qs183::qread, qs191::qread, qs::qread))
  
  print("qs183 lz4 save"); test_compatability(qs183_lz4_save, list(qs183::qread, qs191::qread, qs::qread))
  print("qs183 zstd save"); test_compatability(qs183_zstd_save, list(qs183::qread, qs191::qread, qs::qread))
  print("qs183 zstd stream save"); test_compatability(qs183_zstd_stream_save, list(qs183::qread, qs191::qread, qs::qread))
  print("qs183 zstd stream save no hash"); test_compatability(qs183_zstd_stream_save_nohash, list(qs183::qread, qs191::qread, qs::qread))
  print("qs183 no shuffle save"); test_compatability(qs183_no_shuffle, list(qs183::qread, qs191::qread, qs::qread))
}
# restart from 0.20 on -- we don't have to test all the way back in time indefinitely

print("qs191 lz4 save"); test_compatability(qs191_lz4_save, list(qs183::qread, qs191::qread, qs202::qread, qs::qread))
print("qs191 zstd save"); test_compatability(qs191_zstd_save, list(qs183::qread, qs191::qread, qs202::qread, qs::qread))
print("qs191 zstd stream save"); test_compatability(qs191_zstd_stream_save, list(qs183::qread, qs191::qread, qs202::qread, qs::qread))
print("qs191 zstd stream save no hash"); test_compatability(qs191_zstd_stream_save_nohash, list(qs183::qread, qs191::qread, qs202::qread, qs::qread))
print("qs191 no shuffle save"); test_compatability(qs191_no_shuffle, list(qs183::qread, qs191::qread, qs202::qread, qs::qread))

print("qs202 lz4 save"); test_compatability(qs191_lz4_save, list(qs183::qread, qs191::qread, qs202::qread, qs::qread))
print("qs202 zstd save"); test_compatability(qs191_zstd_save, list(qs183::qread, qs191::qread, qs202::qread, qs::qread))
print("qs202 zstd stream save"); test_compatability(qs191_zstd_stream_save, list(qs183::qread, qs191::qread, qs202::qread, qs::qread))
print("qs202 zstd stream save no hash"); test_compatability(qs191_zstd_stream_save_nohash, list(qs183::qread, qs191::qread, qs202::qread, qs::qread))
print("qs202 no shuffle save"); test_compatability(qs191_no_shuffle, list(qs183::qread, qs191::qread, qs202::qread, qs::qread))


