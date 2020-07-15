suppressMessages(library(Rcpp))
suppressMessages(library(dplyr))
suppressMessages(library(data.table))
suppressMessages(library(qs))
suppressMessages(library(stringfish))
options(warn=1)

args <- commandArgs(T)
if(length(args) == 0) {
  mode <- "filestream"
} else {
  mode <- args[1] # fd, memory, HANDLE
}
if(length(args) < 2) {
  reps <- 5
} else {
  reps <- as.numeric(args[2])
}

Sys.setenv("PKG_CXXFLAGS"="-std=c++11")
cppFunction("CharacterVector splitstr(std::string x, std::vector<double> cuts){
            CharacterVector ret(cuts.size() - 1);
            for(uint64_t i=1; i<cuts.size(); i++) {
              ret[i-1] = x.substr(std::round(cuts[i-1])-1, std::round(cuts[i])-std::round(cuts[i-1]));
            }
            return ret;
            }")

cppFunction('
  int setlev(SEXP x, int i) {
    return SETLEVELS(x,i);
  }')

cppFunction('
void setobj(SEXP x, int i) {
  return SET_OBJECT(x, i);
}')

# https://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
cppFunction('List generateList(std::vector<int> list_elements){
            auto randchar = []() -> char
            {
                const char charset[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
                const size_t max_index = (sizeof(charset) - 1);
                return charset[ rand() % max_index ];
            };
            List ret(list_elements.size());
            std::string str(10,0);
            for(size_t i=0; i<list_elements.size(); i++) {
              switch(list_elements[i]) {
                case 1:
                  ret[i] = R_NilValue;
                  break;
                case 2:
                  std::generate_n( str.begin(), 10, randchar );
                  ret[i] = str;
                  break;
                case 3:
                  ret[i] = rand();
                  break;
                case 4:
                  ret[i] = static_cast<double>(rand());
                  break;
                }
              }
            return ret;
            }')

obj_size <- 0; max_size <- 1e7
get_obj_size <- function() {
  get("obj_size", envir=globalenv())
}
set_obj_size <- function(x) {
  assign("obj_size", get_obj_size() + as.numeric(object.size(x)), envir=globalenv())
  # printCarriage(get_obj_size());
  return(get_obj_size());
}
random_object_generator <- function(N, with_envs = FALSE) { # additional input: global obj_size, max_size
  if(sample(3,1) == 1) {
    ret <- as.list(1:N)
  } else if(sample(2,1) == 1) {
    ret <- as.pairlist(1:N)
  } else {
    ret <- as.pairlist(1:N)
    setlev(ret, sample(2L^12L,1L) - 1L)
    setobj(ret,1L)
  }

  for(i in 1:N) {
    if(get_obj_size() > get("max_size", envir=globalenv())) break;
    otype <- sample(12, size=1)
    z <- NULL
    is_attribute <- ifelse(i == 1, F, sample(c(F,T),size=1))
    if(otype == 1) {z <- rnorm(1e4); set_obj_size(z);}
    else if(otype == 2) { z <- sample(1e4)-5e2; set_obj_size(z); }
    else if(otype == 3) { z <- sample(c(T,F,NA), size=1e4, replace=T); set_obj_size(z); }
    else if(otype == 4) { z <- (sample(256, size=1e4, replace=T)-1) %>% as.raw; set_obj_size(z); }
    else if(otype == 5) { z <- replicate(sample(1e4,size=1), {rep(letters, length.out=sample(10, size=1)) %>% paste(collapse="")}); set_obj_size(z); }
    else if(otype == 6) { z <- rep(letters, length.out=sample(1e4, size=1)) %>% paste(collapse=""); set_obj_size(z); }
    else if(otype == 7) { z <- as.formula("y ~ a + b + c : d", env=globalenv()); attr(z, "blah") <-sample(1e4)-5e2; set_obj_size(z); }
    else if(with_envs && otype %in% c(8,9)) { z <- function(x) {x + runif(1)} }
    # else if(with_envs && otype %in% c(10,11)) { z <- new.env(); z$x <- random_object_generator(N, with_envs); makeActiveBinding("y", function() runif(1), z) }
    else { z <- random_object_generator(N, with_envs) }
    if(is_attribute) {
      attr(ret[[i-1]], runif(1) %>% as.character()) <- z
    } else {
      ret[[i]] <- z
    }
  }
  return(ret)
}

rand_strings <- function(n) {
  s <- sample(0:100, size=n, replace=T)
  x <- lapply(unique(s), function(si) {
    qs::randomStrings(sum(s == si), si)
  }) %>% unlist %>% sample
  x[sample(n, size=n/10)] <- NA
  return(x)
}

nested_tibble <- function() {
  sub_tibble <- function(nr=600, nc=4) {
    z <- lapply(1:nc, function(i) rand_strings(nr)) %>% bind_cols %>% setNames(make.unique(paste0(sample(letters, nc), rand_strings(nc)))) %>% as_tibble
  }
  tibble(
    col1 = rand_strings(100),
    col2 = rand_strings(100),
    col3 = lapply(1:100, function(i) sub_tibble(nr=600, nc=4)),
    col4 = lapply(1:100, function(i) sub_tibble(nr=600, nc=4)),
    col5 = lapply(1:100, function(i) sub_tibble(nr=600, nc=4))
  ) %>% setNames(make.unique(paste0(sample(letters, 5), rand_strings(5))))
}

test_points <- c(0, 1,2,4,8, 2^5-1, 2^5+1, 2^5,2^8-1, 2^8+1,2^8,2^16-1, 2^16+1, 2^16, 1e6, 1e7)
extra_test_points <- c(2^32-1, 2^32+1, 2^32) # not enough memory on desktop

if (Sys.info()[['sysname']] != "Windows") {
  myfile <- "/tmp/ctest.z"
} else {
  myfile <- "N:/ctest.z"
}

printCarriage <- function(x) {
  cat(x, "\r")
}

serialize_identical <- function(x1, x2) {
  identical(serialize(x1, NULL), serialize(x2, NULL))
}
################################################################################################
# some one off tests

# test 1: alt rep implementation
# https://github.com/traversc/qs/issues/9

x <- data.table(x = 1:26, y = letters)
qsave(x, file=myfile)
xu <- qread(myfile, use_alt_rep = T)
data.table::setnames(xu, 1, "a")
stopifnot(identical(c("a", "y"), colnames(xu)))
data.table::setnames(xu, 2, "b")
stopifnot(identical(c("a", "b"), colnames(xu)))

################################################################################################

qsave_rand <- function(x, file) {
  alg <- sample(c("lz4", "zstd", "lz4hc", "zstd_stream", "uncompressed"), 1)
  # alg <- "zstd_stream"
  nt <- sample(5,1)
  sc <- sample(0:15,1)
  cl <- sample(10,1)
  ch <- sample(c(T,F),1)
  if(mode == "filestream") {
    qsave(x, file=file, preset = "custom", algorithm = alg,
        compress_level=cl, shuffle_control = sc, nthreads=nt, check_hash = ch)
  } else if(mode == "fd") {
    fd <- qs:::openFd(myfile, "w")
    qsave_fd(x, fd, preset = "custom", algorithm = alg,
          compress_level=cl, shuffle_control = sc, check_hash = ch)
    qs:::closeFd(fd)
  } else if(mode == "handle") {
    h <- qs:::openHandle(myfile, "w")
    qsave_handle(x, h, preset = "custom", algorithm = alg,
             compress_level=cl, shuffle_control = sc, check_hash = ch)
    qs:::closeHandle(h)
  } else if(mode == "memory") {
    .sobj <<- qserialize(x, preset = "custom", algorithm = alg,
                         compress_level=cl, shuffle_control = sc, check_hash = ch)
  } else {
    stop(paste0("wrong write-mode selected: ", mode))
  }
}

qread_rand <- function(file) {
  ar <- sample(c(T,F),1)
  nt <- sample(5,1)
  if(mode == "filestream") {
    x <- qread(file, use_alt_rep=ar, nthreads=nt, strict=T)
  } else if(mode == "fd") {
    if(sample(2,1) == 1) {
      fd <- qs:::openFd(myfile, "r")
      x <- qread_fd(fd, use_alt_rep=ar, strict=T)
      qs:::closeFd(fd)
    } else {
      x <- qread(file, use_alt_rep=ar, nthreads=nt, strict=T)
    }
  } else if(mode == "handle") {
    if(sample(2,1) == 1) {
      h <- qs:::openHandle(myfile, "r")
      x <- qread_handle(h, use_alt_rep=ar, strict=T)
      qs:::closeHandle(h)
    } else {
      x <- qread(file, use_alt_rep=ar, nthreads=nt, strict=T)
    }
  } else if(mode == "memory") {
    x <- qdeserialize(.sobj, use_alt_rep=ar, strict=T)
  } else {
    stop(paste0("wrong read-mode selected: ", mode))
  }
  return(x)
}

################################################################################################

for(q in 1:reps) {
  cat(q, "\n")
  
  # String correctness
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      x1 <- rep(letters, length.out=tp) %>% paste(collapse="")
      x1 <- c(NA, "", x1)
      time[i] <- Sys.time()
      qsave_rand(x1, file=myfile)
      z <- qread_rand(file=myfile)
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("strings: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")
  
  # Character vectors
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      # qs_use_alt_rep(F)
      x1 <- rep(as.raw(sample(255)), length.out = tp*10) %>% rawToChar
      cuts <- sample(tp*10, tp+1) %>% sort %>% as.numeric
      x1 <- splitstr(x1, cuts)
      x1 <- c(NA, "", x1)
      qsave_rand(x1, file=myfile)
      time[i] <- Sys.time()
      z <- qread_rand(file=myfile)
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Character Vectors: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")
  
  # stringfish character vectors
  # more tests 
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      # qs_use_alt_rep(F)
      x1 <- rep(as.raw(sample(255)), length.out = tp*10) %>% rawToChar
      cuts <- sample(tp*10, tp+1) %>% sort %>% as.numeric
      x1 <- splitstr(x1, cuts)
      x1 <- c(NA, "", x1)
      x1 <- stringfish::convert_to_sf(x1)
      qsave_rand(x1, file=myfile)
      time[i] <- Sys.time()
      z <- qread_rand(file=myfile)
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Stringfish: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")
  
  # Integers
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      x1 <- sample(1:tp, replace=T)
      x1 <- c(NA, x1)
      time[i] <- Sys.time()
      qsave_rand(x1, file=myfile)
      z <- qread_rand(file=myfile)
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Integers: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")
  
  # Doubles
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      x1 <- rnorm(tp)
      x1 <- c(NA, x1)
      time[i] <- Sys.time()
      qsave_rand(x1, file=myfile)
      z <- qread_rand(file=myfile)
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Numeric: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")
  
  # Logical
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      
      x1 <- sample(c(T,F,NA), replace=T, size=tp)
      time[i] <- Sys.time()
      qsave_rand(x1, file=myfile)
      z <- qread_rand(file=myfile)
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Logical: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")
  
  # List
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      x1 <- generateList(sample(1:4, replace=T, size=tp))
      time[i] <- Sys.time()
      qsave_rand(x1, file=myfile)
      z <- qread_rand(file=myfile)
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("List: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")
  
  for(i in 1:3) {
    x1 <- rep( replicate(1000, { rep(letters, length.out=2^7+sample(10, size=1)) %>% paste(collapse="") }), length.out=1e6 )
    x1 <- data.frame(str=x1,num = runif(1:1000), stringsAsFactors = F)
    qsave_rand(x1, file=myfile)
    z <- qread_rand(file=myfile)
    gc()
    stopifnot(identical(z, x1))
  }
  cat("Data.frame test")
  cat("\n")
  
  for(i in 1:3) {
    x1 <- rep( replicate(1000, { rep(letters, length.out=2^7+sample(10, size=1)) %>% paste(collapse="") }), length.out=1e6 )
    x1 <- data.table(str=x1,num = runif(1:1e6))
    qsave_rand(x1, file=myfile)
    z <- qread_rand(file=myfile)
    gc()
    stopifnot(all(z==x1))
  }
  cat("Data.table test")
  cat("\n")
  
  for(i in 1:3) {
    
    x1 <- rep( replicate(1000, { rep(letters, length.out=2^7+sample(10, size=1)) %>% paste(collapse="") }), length.out=1e6 )
    x1 <- tibble(str=x1,num = runif(1:1e6))
    qsave_rand(x1, file=myfile)
    z <- qread_rand(file=myfile)
    gc()
    stopifnot(identical(z, x1))
  }
  cat("Tibble test")
  cat("\n")
  
  # Encoding test
  if (Sys.info()[['sysname']] != "Windows") {
    for(i in 1:3) {
      
      x1 <- "己所不欲，勿施于人" # utf 8
      x2 <- x1
      Encoding(x2) <- "latin1"
      x3 <- x1
      Encoding(x3) <- "bytes"
      x4 <- rep(x1, x2, length.out=1e4) %>% paste(collapse=";")
      x1 <- c(x1, x2, x3, x4)
      qsave_rand(x1, file=myfile)
      z <- qread_rand(file=myfile)
      gc()
      stopifnot(identical(z, x1))
    }
    printCarriage("Encoding test")
  } else {
    printCarriage("(Encoding test not run on windows)")
  }
  cat("\n")
  
  # complex vectors
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      
      re <- rnorm(tp)
      im <- runif(tp)
      x1 <- complex(real=re, imaginary=im)
      x1 <- c(NA_complex_, x1)
      time[i] <- Sys.time()
      qsave_rand(x1, file=myfile)
      z <- qread_rand(file=myfile)
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Complex: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")
  
  # factors
  for(tp in test_points) {
    time <- vector("numeric", length=3)
    for(i in 1:3) {
      x1 <- factor(rep(letters, length.out=tp), levels=sample(letters), ordered=TRUE)
      time[i] <- Sys.time()
      qsave_rand(x1, file=myfile)
      z <- qread_rand(file=myfile)
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    printCarriage(sprintf("Factors: %s, %s s",tp, signif(mean(time),4)))
  }
  cat("\n")

  # nested lists
  time <- vector("numeric", length=8)
  for(i in 1:8) {
    # qs_use_alt_rep(sample(c(T,F), size=1))
    obj_size <- 0
    x1 <- random_object_generator(12)
    printCarriage(sprintf("Nested list/attributes: %s bytes", object.size(x1) %>% as.numeric))
    time[i] <- Sys.time()
    qsave_rand(x1, file=myfile)
    z <- qread_rand(file=myfile)
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  printCarriage(sprintf("Nested list/attributes: %s s", signif(mean(time),4)))
  cat("\n")
  
  # nested attributes
  time <- vector("numeric", length=3)
  for(i in 1:3) {
    x1 <- as.list(1:26)
    attr(x1[[26]], letters[26]) <- rnorm(100)
    for(i in 25:1) {
      attr(x1[[i]], letters[i]) <- x1[[i+1]]
    }
    time[i] <- Sys.time()
    qsave_rand(x1, file=myfile)
    z <- qread_rand(file=myfile)
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  printCarriage(sprintf("Nested attributes: %s s", signif(mean(time),4)))
  cat("\n")
  
  # alt-rep -- should serialize the unpacked object
  time <- vector("numeric", length=3)
  for(i in 1:3) {
    x1 <- 1:1e7
    time[i] <- Sys.time()
    qsave_rand(x1, file=myfile)
    z <- qread_rand(file=myfile)
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  printCarriage(sprintf("Alt rep integer: %s s", signif(mean(time),4)))
  cat("\n")
  
  
  # Environment test
  time <- vector("numeric", length=3)
  for(i in 1:3) {
    x1 <- new.env()
    x1[["a"]] <- 1:1e7
    x1[["b"]] <- runif(1e7)
    x1[["c"]] <- qs::randomStrings(1e4)
    time[i] <- Sys.time()
    qsave_rand(x1, file=myfile)
    z <- qread_rand(file=myfile)
    stopifnot(identical(z[["a"]], x1[["a"]]))
    stopifnot(identical(z[["b"]], x1[["b"]]))
    stopifnot(identical(z[["c"]], x1[["c"]]))
    time[i] <- Sys.time() - time[i]
    gc()
  }
  printCarriage(sprintf("Environment test: %s s", signif(mean(time),4)))
  cat("\n")
  
  time <- vector("numeric", length=3)
  for(i in 1:3) {
    x1 <- suppressMessages(nested_tibble())
    time[i] <- Sys.time()
    qsave_rand(x1, file=myfile)
    z <- qread_rand(file=myfile)
    stopifnot(identical(z, x1))
    time[i] <- Sys.time() - time[i]
    gc()
  }
  printCarriage(sprintf("nested tibble test: %s s", signif(mean(time),4)))
  cat("\n")
}

cat("tests done")
rm(list=ls())
gc()



