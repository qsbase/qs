suppressMessages(library(Rcpp))
suppressMessages(library(dplyr))
suppressMessages(library(data.table))
suppressMessages(library(qs))
options(warn=1)

Sys.setenv("PKG_CXXFLAGS"="-std=c++11")
cppFunction("CharacterVector splitstr(std::string x, std::vector<double> cuts){
            CharacterVector ret(cuts.size() - 1);
            for(uint64_t i=1; i<cuts.size(); i++) {
              ret[i-1] = x.substr(std::round(cuts[i-1])-1, std::round(cuts[i])-std::round(cuts[i-1]));
            }
            return ret;
            }")

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
  # print(get_obj_size());
  return(get_obj_size());
}
random_object_generator <- function(N) { # additional input: global obj_size, max_size
  ret <- as.list(1:N)
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
    else { z <- random_object_generator(N) }
    if(is_attribute) {
      attr(ret[[i-1]], runif(1) %>% as.character()) <- z
    } else {
      ret[[i]] <- z
    }
  }
  return(ret)
}

test_points <- c(0, 1,2,4,8, 2^5-1, 2^5+1, 2^5,2^8-1, 2^8+1,2^8,2^16-1, 2^16+1, 2^16, 1e6, 1e7)
extra_test_points <- c(2^32-1, 2^32+1, 2^32) # not enough memory on desktop
reps <- 10

################################################################################################

qsave_rand <- function(x, file) {
  qsave(x, file="/tmp/ctest.z", preset = "custom", algorithm = sample(c("lz4", "zstd", "lz4hc", "zstd_stream"), 1),
        compress_level=sample(10,1), shuffle_control = sample(0:15,1), nthreads=sample(5,1) )
}

# qsave_rand <- function(x, file) {
#   qsave(x, file="/tmp/ctest.z", preset = "custom", algorithm = "zstd_stream",
#         compress_level=sample(10,1), shuffle_control = sample(0:15,1), nthreads=sample(5,1) )
# }

qread_rand <- function(x, file) {
  qread("/tmp/ctest.z", 
        use_alt_rep = sample(c(T,F),1),inspect=T,
        nthreads=sample(5,1))
}

for(q in 1:reps) {

# String correctness
time <- vector("numeric", length=3)
for(tp in test_points) {
  for(i in 1:3) {
    x1 <- rep(letters, length.out=tp) %>% paste(collapse="")
    x1 <- c(NA, "", x1)
    time[i] <- Sys.time()
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  print(sprintf("strings: %s, %s s",tp, signif(mean(time),4)))
}

# Character vectors
time <- vector("numeric", length=3)
for(tp in test_points) {
  for(i in 1:3) {
    # qs_use_alt_rep(F)
    x1 <- rep(as.raw(sample(255)), length.out = tp*10) %>% rawToChar
    cuts <- sample(tp*10, tp+1) %>% sort %>% as.numeric
    x1 <- splitstr(x1, cuts)
    x1 <- c(NA, "", x1)
    qsave_rand(x1, file="/tmp/test.z")
    time[i] <- Sys.time()
    z <- qread_rand(file="/tmp/test.z")
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  print(sprintf("Character Vectors: %s, %s s",tp, signif(mean(time),4)))
}

# Integers
time <- vector("numeric", length=3)
for(tp in test_points) {
  for(i in 1:3) {
    x1 <- sample(1:tp, replace=T)
    x1 <- c(NA, x1)
    time[i] <- Sys.time()
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  print(sprintf("Integers: %s, %s s",tp, signif(mean(time),4)))
}

# Doubles
time <- vector("numeric", length=3)
for(tp in test_points) {
  for(i in 1:3) {
    
    x1 <- rnorm(tp)
    x1 <- c(NA, x1)
    time[i] <- Sys.time()
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  print(sprintf("Numeric: %s, %s s",tp, signif(mean(time),4)))
}

# Logical
time <- vector("numeric", length=3)
for(tp in test_points) {
  for(i in 1:3) {
    
    x1 <- sample(c(T,F,NA), replace=T, size=tp)
    time[i] <- Sys.time()
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  print(sprintf("Logical: %s, %s s",tp, signif(mean(time),4)))
}

# List
time <- vector("numeric", length=3)
for(tp in test_points) {
  for(i in 1:3) {
    x1 <- generateList(sample(1:4, replace=T, size=tp))
    time[i] <- Sys.time()
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  print(sprintf("List: %s, %s s",tp, signif(mean(time),4)))
}

for(i in 1:3) {
  x1 <- rep( replicate(1000, { rep(letters, length.out=2^7+sample(10, size=1)) %>% paste(collapse="") }), length.out=1e6 )
  x1 <- data.frame(str=x1,num = runif(1:1000), stringsAsFactors = F)
  qsave_rand(x1, file="/tmp/test.z")
  z <- qread_rand(file="/tmp/test.z")
  gc()
  stopifnot(identical(z, x1))
}
print("Data.frame test")

for(i in 1:3) {
  x1 <- rep( replicate(1000, { rep(letters, length.out=2^7+sample(10, size=1)) %>% paste(collapse="") }), length.out=1e6 )
  x1 <- data.table(str=x1,num = runif(1:1e6))
  qsave_rand(x1, file="/tmp/test.z")
  z <- qread_rand(file="/tmp/test.z")
  gc()
  stopifnot(all(z==x1))
}
print("Data.table test")

for(i in 1:3) {
  
  x1 <- rep( replicate(1000, { rep(letters, length.out=2^7+sample(10, size=1)) %>% paste(collapse="") }), length.out=1e6 )
  x1 <- tibble(str=x1,num = runif(1:1e6))
  qsave_rand(x1, file="/tmp/test.z")
  z <- qread_rand(file="/tmp/test.z")
  gc()
  stopifnot(identical(z, x1))
}
print("Tibble test")

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
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    gc()
    stopifnot(identical(z, x1))
  }
  print("Encoding test")
} else {
  print("(Encoding test not run on windows)")
}

# complex vectors
time <- vector("numeric", length=3)
for(tp in test_points) {
  for(i in 1:3) {
    
    re <- rnorm(tp)
    im <- runif(tp)
    x1 <- complex(real=re, imaginary=im)
    x1 <- c(NA_complex_, x1)
    time[i] <- Sys.time()
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  print(sprintf("Complex: %s, %s s",tp, signif(mean(time),4)))
}

# factors
for(tp in test_points) {
  time <- vector("numeric", length=3)
  for(i in 1:3) {
    x1 <- factor(rep(letters, length.out=tp), levels=sample(letters), ordered=TRUE)
    time[i] <- Sys.time()
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  print(sprintf("Factors: %s, %s s",tp, signif(mean(time),4)))
}

# nested lists
time <- vector("numeric", length=8)
for(i in 1:8) {
  # qs_use_alt_rep(sample(c(T,F), size=1))
  obj_size <- 0
  x1 <- random_object_generator(12)
  print(sprintf("Nested list/attributes: %s bytes", object.size(x1) %>% as.numeric))
  time[i] <- Sys.time()
  qsave_rand(x1, file="/tmp/test.z")
  z <- qread_rand(file="/tmp/test.z")
  time[i] <- Sys.time() - time[i]
  gc()
  stopifnot(identical(z, x1))
}
print(sprintf("Nested list/attributes: %s s", signif(mean(time),4)))

# nested attributes
time <- vector("numeric", length=3)
for(i in 1:3) {
  x1 <- as.list(1:26)
  attr(x1[[26]], letters[26]) <- rnorm(100)
  for(i in 25:1) {
    attr(x1[[i]], letters[i]) <- x1[[i+1]]
  }
  time[i] <- Sys.time()
  qsave_rand(x1, file="/tmp/test.z")
  z <- qread_rand(file="/tmp/test.z")
  time[i] <- Sys.time() - time[i]
  gc()
  stopifnot(identical(z, x1))
}
print(sprintf("Nested attributes: %s s", signif(mean(time),4)))

# alt-rep -- should serialize the unpacked object
time <- vector("numeric", length=3)
for(i in 1:3) {
  x1 <- 1:1e7
  time[i] <- Sys.time()
  qsave_rand(x1, file="/tmp/test.z")
  z <- qread_rand(file="/tmp/test.z")
  time[i] <- Sys.time() - time[i]
  gc()
  stopifnot(identical(z, x1))
}
print(sprintf("Alt rep integer: %s s", signif(mean(time),4)))


# Environment test
time <- vector("numeric", length=3)
for(i in 1:3) {
  x1 <- new.env()
  x1[["a"]] <- 1:1e7
  x1[["b"]] <- runif(1e7)
  x1[["c"]] <- qs::randomStrings(1e4)
  time[i] <- Sys.time()
  qsave_rand(x1, file="/tmp/test.z")
  z <- qread_rand(file="/tmp/test.z")
  stopifnot(identical(z[["a"]], x1[["a"]]))
  stopifnot(identical(z[["b"]], x1[["b"]]))
  stopifnot(identical(z[["c"]], x1[["c"]]))
  time[i] <- Sys.time() - time[i]
  gc()
}
print(sprintf("Environment test: %s s", signif(mean(time),4)))

}