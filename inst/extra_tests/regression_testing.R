install_as_name <- function (pkg, new_name) {
  require(dplyr)
  require(Rcpp)
  previous_dir <- getwd()
  tempfolder <- tempfile()
  dir.create(tempfolder, showWarnings = F)
  setwd(tempfolder)
  print(tempfolder)
  sprintf("wget %s", pkg) %>% system
  print(tempfolder)
  zfolder <- untar(basename(pkg), list = T)
  untar(basename(pkg))
  zfolder <- zfolder %>% gsub("/.*$", "", .) %>% unique
  zfiles <- list.files(zfolder, recursive = T, full.names = T)
  print(zfiles)
  zdesc <- zfiles[basename(zfiles) == "DESCRIPTION"]
  print(zdesc)
  stopifnot(length(zdesc) == 1)
  stopifnot(length(zfolder) == 1)
  x <- readLines(zdesc)
  x <- gsub("^Package.+", sprintf("Package: %s", new_name), x)
  writeLines(x, con = zdesc)
  x <- readLines(paste0(zfolder, "/NAMESPACE"))
  x <- gsub("^useDynLib.+", sprintf("useDynLib(%s, .registration = TRUE)",
                                    new_name), x)
  writeLines(x, con = paste0(zfolder, "/NAMESPACE"))

  zR <- grep("*.R$", zfiles, value=T)
  for(i in 1:length(zR)) {
    x <- readLines(zR[i])
    x <- gsub(sprintf(".Call\\(`_%s_", zfolder), sprintf(".Call\\(`_%s_", new_name), x)
    writeLines(x, con = zR[i])
  }

  sprintf("mv %s %s", zfolder, new_name) %>% system
  Rcpp::compileAttributes(new_name)
  file.remove(sprintf("%s/MD5", new_name))
  "rm *.tar.gz" %>% system
  sprintf("R CMD build %s", new_name) %>% system
  "R CMD INSTALL *.tar.gz" %>% system
  setwd(previous_dir)
}

if(F) {
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.12.tar.gz", "qs12") # zstd block compress only
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.13.1.tar.gz", "qs131") # zstd block compress only
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.14.1.tar.gz", "qs141") # zstd and lz4 block compress, byte shuffling
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.15.1.tar.gz", "qs151") # zstd, lz4, lz4hc block compress
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.16.1.tar.gz", "qs161") # zstd, lz4, lz4hc block compress, zstd_stream compress
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.17.3.tar.gz", "qs173") # zstd, lz4, lz4hc block compress, zstd_stream compress
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.18.3.tar.gz", "qs183") # zstd, lz4, lz4hc block compress, zstd_stream compress
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.19.1.tar.gz", "qs191") # zstd, lz4, lz4hc block compress, zstd_stream compress
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.20.2.tar.gz", "qs202") # zstd, lz4, lz4hc block compress, zstd_stream compress
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.21.2.tar.gz", "qs212") # zstd, lz4, lz4hc block compress, zstd_stream compress
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.22.1.tar.gz", "qs221") # zstd, lz4, lz4hc block compress, zstd_stream compress
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.23.6.tar.gz", "qs236") # zstd, lz4, lz4hc block compress, zstd_stream compress
  # install_as_name("https://cran.r-project.org/src/contrib/Archive/qs/qs_0.24.1.tar.gz", "qs241") # zstd, lz4, lz4hc block compress, zstd_stream compress
  # install_as_name("https://cran.r-project.org/src/contrib/qs_0.25.7.tar.gz", "qs257")
  # Only qs +0.25 can be installed on R 4.2 due to changes in C API
  install_as_name(pkg = "https://cran.r-project.org/src/contrib/Archive/qs/qs_0.25.3.tar.gz", new_name = "qs257") # zstd, lz4, lz4hc block compress, zstd_stream compress

  # Earlier version cannot read zstd_stream from 0.17.1+ due to additional checksum at end of file
  # qs 0.18.1 -- header version 2 -- will not be readable by earlier versions

  # test if pacakges can be loaded
  # library(qs12)
  # library(qs131)
  # library(qs141)
  # library(qs151)
  # library(qs161)
  # library(qs173)
  # library(qs183)
  # library(qs191)
  # library(qs202)
  # library(qs212)
  # library(qs221)
  # library(qs236)
}

library(qs257)
library(qs) # 0.26.1

file <- tempfile()

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

serialize_identical <- function(x, y) {
  identical(serialize(x, connection = NULL), serialize(y, connection = NULL))
}

test_ext_compatability <- function(save_funs, read_funs) {
  x <- new.env()
  # https://colinfay.me/ractivebinfing/
  makeActiveBinding(sym = "classy_word",
                    fun = function(value){
                      if (missing(value)) {
                        sample(c("Classy","Modish", "High-Class","Dashing","Posh"), 1)
                      } else {
                        cat(paste("Your classy word is", value))
                      }
                    },
                    env = x)
  x$a <- function(a) {a + 1}
  environment(x$a) <- globalenv()

  res <- list()
  grid <- expand.grid(i = 1:length(save_funs), j = 1:length(read_funs))
  for(q in 1:nrow(grid)) {
    print(q)
    save_funs[[grid$i[q]]](x)
    res[[q]] <- read_funs[[grid$j[q]]](file)
  }
  for(q in 2:length(res)) {
    cat(q)
    serialize_identical(res[[1]], res[[q]])
  }
  cat("\n")
}

# restart from 25.3
qs257_lz4_save <- function(x) qs257::qsave(x, file, preset = "custom", algorithm = "lz4")
qs257_zstd_save <- function(x) qs257::qsave(x, file, preset = "custom", algorithm = "zstd")
qs257_zstd_stream_save <- function(x) qs257::qsave(x, file, preset = "custom", algorithm = "zstd_stream")
qs257_zstd_stream_save_nohash <- function(x) qs::qsave(x, file, preset = "custom", algorithm = "zstd_stream", check_hash = F)
qs257_no_shuffle <- function(x) qs::qsave(x, file, preset = "custom", algorithm = "zstd", shuffle_control = 0)

print("qs257 lz4 save"); test_compatability(qs257_lz4_save, list(qs::qread))
print("qs257 zstd save"); test_compatability(qs257_zstd_save, list(qs::qread))
print("qs257 zstd stream save"); test_compatability(qs257_zstd_stream_save, list(qs::qread))
print("qs257 zstd stream save no hash"); test_compatability(qs257_zstd_stream_save_nohash, list(qs::qread))
print("qs257 no shuffle save"); test_compatability(qs257_no_shuffle, list(qs::qread))

# Test new environment saving using findVarInFrame
save_funs <- c(qs257_lz4_save, qs257_zstd_save, qs257_zstd_stream_save)
read_funs <- c(qs257::qread, qs::qread)
print("Testing new environment saving"); test_ext_compatability(save_funs, read_funs)


